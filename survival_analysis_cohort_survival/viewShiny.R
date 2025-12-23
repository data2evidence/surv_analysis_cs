library(OhdsiShinyAppBuilder)
library(dplyr)
library(shiny)
library(readr)
library(omopgenerics)
library(CohortSurvival)


########### Survival Analysis module ##############################
survivalModuleUI <- function(id) {
  ns <- NS(id)  # Namespace for the module
  fluidPage(
    # Dropdown to choose a dataset or option
    selectInput(
      inputId = ns("dataset"),      # namespaced ID
      label   = "Choose a dataset", # label shown to user
      choices = NULL
    ),
    # Survival plot output
    plotOutput(ns("km_plot"))
  )
}
survivalModuleServer <- function(id, resultDatabaseSettings, connectionHandler) {
  moduleServer(id, function(input, output, session) {
    dataset_choices <- reactive({
      conn <- connectionHandler$getConnection()
      res <- DatabaseConnector::querySql(
        conn,
        paste0(
          "SELECT DISTINCT cdm_name AS database_id
              from ", resultsDatabaseSchema, ".cs_survival_results
             WHERE cdm_name IS NOT NULL"
        )
      )
      return(res$database_id)
    })
    observeEvent(dataset_choices(), {
      updateSelectInput(
        session,
        inputId = "dataset",
        choices = dataset_choices(),
        selected = dataset_choices()[1]
      )
    })
    output$km_plot <- renderPlot({
        req(input$dataset)  # Wait for dataset to be selected
        conn <- connectionHandler$getConnection()
        sql_query <- paste0(
            "SELECT * FROM ", resultsDatabaseSchema, ".cs_survival_results WHERE cdm_name = '", input$dataset, "' OR cdm_name IS NULL"
        )
        print(paste("Executing SQL query:\n", sql_query))
        cs_data <- DatabaseConnector::querySql(
            conn,
            sql_query
            # paste0(
            #   "SELECT *
            #      FROM ", resultsDatabaseSchema, ".cs_survival_results
            #     WHERE DATABASE_ID = '", input$dataset, "'"
            # )
        )
        cs_data$row_id <- NULL
        colnames(cs_data) <- tolower(colnames(cs_data))
        # Write to temporary CSV file
        temp_file <- tempfile(fileext = ".csv")
        on.exit(unlink(temp_file), add = TRUE)  # Clean up when function exits
        readr::write_csv(cs_data, temp_file)
        # Import using omopgenerics function
        cs_data <- omopgenerics::importSummarisedResult(path = temp_file)
        # CohortSurvival::plotSurvival(cs_data)
        
        # Check strata information
        strata_names <- unique(cs_data$strata_name)
        # Determine facet parameter based on strata_name
        # Filter out "overall" and find actual strata
        actual_strata <- strata_names[strata_names != "overall"]

        if (length(actual_strata) > 0) {
            strata_type <- actual_strata[1]  # Use first non-overall strata
            facet_var <- strata_type
        } else {
            strata_type <- NULL
            facet_var <- NULL
        }

        # Plot based on detected strata
        if (!is.null(facet_var)) {
            print(paste("Plotting with facet:", facet_var, "\n"))
            CohortSurvival::plotSurvival(cs_data, facet = facet_var)
            # output_file <- paste0("./survival_plot_", strata_type, ".png")
            # ggplot2::ggsave(output_file, surv_plot, width = 8, height = 6)
            # cat("Plot saved to:", output_file, "\n")
        } else {
            cat("\nPlotting without stratification\n")
            surv_plot <- CohortSurvival::plotSurvival(cs_data)
            # ggplot2::ggsave("./survival_plot.png", surv_plot, width = 8, height = 6)
        }

    })
  })
}

config <- OhdsiShinyAppBuilder::initializeModuleConfig() %>%
        addModuleConfig(createDefaultAboutConfig()) %>%
        addModuleConfig(createDefaultCohortGeneratorConfig() ) %>%
        addModuleConfig(createDefaultCharacterizationConfig()) %>%
        addModuleConfig(createDefaultEstimationConfig()) %>%
        addModuleConfig(
            createModuleConfig(
                moduleId = 'survival',
                tabName = "SurvivalAnalysis",
                shinyModulePackage = NULL,
                shinyModulePackageVersion = NULL,
                moduleUiFunction = survivalModuleUI,
                moduleServerFunction = survivalModuleServer,
                moduleInfoBoxFile = function(){},
                moduleIcon = "info",
                installSource = "CRAN",
                gitHubRepo = NULL
            )
        )

# connect to database with results
dbms   <- "postgresql"  
server <- "127.0.0.1/study_results"  # for PostgreSQL, can be "localhost/DBNAME"
user   <- "study_results_pg_admin_user"
pw     <- "dpQo1Cq0eIXXemiHCglrcOyXHoOoiS"
port   <- 41192              # local forwarded port from your SSH tunnel

results_schema_name <- "results_surv_analysis_cs"
pathToDriver <- "/Users/amit.sharma/Documents/Projects"  # e.g., "/path/to/your/jdbc/driver"
resultsDatabaseSchema <- results_schema_name

# Create connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = dbms,
  server = server,
  user = user,
  password = pw,
  port = port,
  pathToDriver = pathToDriver
)

connection <- ResultModelManager::ConnectionHandler$new(connectionDetails)

OhdsiShinyAppBuilder::viewShiny(
config = config, 
connection = connection
)
