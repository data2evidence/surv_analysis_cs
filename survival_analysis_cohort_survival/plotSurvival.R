library(DatabaseConnector)
library(omopgenerics)
library(CohortSurvival)
library(ggplot2)
library(readr)

# connect to database with results
dbms   <- "postgresql"  
server <- "127.0.0.1/study_results"  # for PostgreSQL, can be "localhost/DBNAME"
user   <- "study_results_pg_admin_user"
pw     <- "dpQo1Cq0eIXXemiHCglrcOyXHoOoiS"
port   <- 41192              # local forwarded port from your SSH tunnel

results_schema_name <- "results_surv_analysis_cs"
pathToDriver <- "/Users/amit.sharma/Documents/Projects"  # e.g., "/path/to/your/jdbc/driver"

# Create connection details
connectionDetails <- createConnectionDetails(
  dbms = dbms,
  server = server,
  user = user,
  password = pw,
  port = port,
  pathToDriver = pathToDriver
)

# read from database 
cat("\nReading results from database \n")
conn <- DatabaseConnector::connect(connectionDetails)
db_query <- paste0("Select * from ", results_schema_name, ".cs_survival_results")
df <- DatabaseConnector::querySql(conn, db_query)
df$row_id <- NULL
# print(head(df))
DatabaseConnector::disconnect(conn)

tmp_in <- tempfile(fileext = ".csv")   
write_csv(df, tmp_in)



cat("Checkpoint 1 ################### \n")
sr2 <- importSummarisedResult(tmp_in)
cat("Checkpoint 2 ################### \n")

# Check strata information
cat("\nUnique strata_name values:\n")
strata_names <- unique(sr2$strata_name)
print(strata_names)

# Determine facet parameter based on strata_name
# Filter out "overall" and find actual strata
actual_strata <- strata_names[strata_names != "overall"]

if (length(actual_strata) > 0) {
  strata_type <- actual_strata[1]  # Use first non-overall strata
  cat("\nDetected strata type:", strata_type, "\n")
  facet_var <- strata_type
} else {
  strata_type <- NULL
  facet_var <- NULL
  cat("\nNo stratification detected (only 'overall')\n")
}

# Plot based on detected strata
if (!is.null(facet_var)) {
  cat("\nPlotting with facet:", facet_var, "\n")
  surv_plot <- plotSurvival(sr2, facet = facet_var)
  output_file <- paste0("./survival_plot_", strata_type, ".png")
  ggplot2::ggsave(output_file, surv_plot, width = 8, height = 6)
  cat("Plot saved to:", output_file, "\n")
} else {
  cat("\nPlotting without stratification\n")
  surv_plot <- plotSurvival(sr2)
  ggplot2::ggsave("./survival_plot.png", surv_plot, width = 8, height = 6)
}

# surv_plot <- plotSurvival(sr2, 
#              colour = "strata_gender",
#              facet = "outcome")  # separate panels per outcome, colored by gender
# ggplot2::ggsave("./survival_plot_facet_outcome.png", surv_plot, width = 8, height = 6)