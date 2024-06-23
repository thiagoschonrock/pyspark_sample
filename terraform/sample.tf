resource "google_cloudbuildv2_repository" "pyspark_sample_repo" {
  location          = "us-west1"
  name              = "dbt-bigquery"
  parent_connection = "connections/1234567890123456789"
  remote_uri        = "https://github.com/ebanx/dbt-bigquery.git"
}

resource "google_cloudbuild_trigger" "build_pyspark_sample" {
  location    = "us-west1"
  name        = "build_pyspark_sample"

  repository_event_config {
    repository = google_cloudbuildv2_repository.dbt_bigquery.id
    push {
      branch = "^main$"
    }
  }

  included_files = ["cicd/cloudbuild-cd-create-dbt-image-dev.yaml"]

  filename = "cicd/cloudbuild-cd-create-dbt-image-dev.yaml"
}

resource "google_cloudbuild_trigger" "build_pyspark_job" {
  location    = "us-west1"
  name        = "build_pyspark_job"

  repository_event_config {
    repository = google_cloudbuildv2_repository.dbt_bigquery.id
    push {
      branch = "^main$"
    }
  }

  included_files = ["**"]

  filename = "cicd/cloudbuild-cd-generate-dbt-artifacts-dev.yaml"
}