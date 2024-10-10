import pandas as pd
from asreview import open_state, ASReviewProject, ASReviewData
from pathlib import Path
import tempfile

class extract:
    @staticmethod
    def extract_and_output(project_file_name, output_path=None, file_format="xlsx"):
        """
        Extract data from ASReview state, merge it with the original dataset, and sort it.

        Parameters:
        - project_file_name: Path to the ASReview project file.
        - output_path: Optional output path to save the merged dataset.
        - file_format: Output file format, either 'xlsx' or 'csv'.

        Returns:
        - merged_dataset: Merged and sorted pandas DataFrame.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_project_path = Path(temp_dir)

            # Load the project and access project details
            project = ASReviewProject.load(project_file_name, temp_project_path)
            dataset_fp = Path(temp_project_path, project.config["id"], "data", project.config["dataset_path"])
            dataset = ASReviewData.from_file(dataset_fp)
            dataset_df = dataset.to_dataframe()

            # Access the review data from the state file
            with open_state(project_file_name) as state:
                df = state.get_dataset()

                # Drop the 'notes' column from df to avoid conflicts
                df = df.drop(columns=['notes'], errors='ignore')
                
                df["labeling_order"] = df.index
                df_state = dataset_df.join(df.set_index("record_id"), on="record_id", how="left")

                # Get ranking and probabilities
                last_ranking = state.get_last_ranking()
                last_probabilities = state.get_last_probabilities()

                # Drop overlapping columns from last_ranking to avoid conflicts during the join
                last_ranking = last_ranking.drop(
                    columns=['classifier', 'query_strategy', 'balance_strategy', 
                             'feature_extraction', 'training_set'],
                    errors='ignore'
                )

                # Ensure that 'record_id' is used as the index for joining
                last_probabilities.index.name = "record_id"
                last_ranking.set_index("record_id", inplace=True)

            # Merge state information into the original dataset step by step
            merged_dataset = df_state.join(last_probabilities, on="record_id", how="left")
            merged_dataset = merged_dataset.join(last_ranking, on="record_id", how="left")

            # Sort the merged dataset based on 'training_set'
            merged_dataset = merged_dataset.sort_values(by='training_set', ascending=True)

            print(f"The state contains {len(df_state)} records.")
            print(merged_dataset.head())
            print(f"The merged dataset contains {len(merged_dataset)} records.")

            # Save as Excel or CSV based on the specified format
            if output_path:
                if file_format.lower() == "xlsx":
                    merged_dataset.to_excel(output_path, index=False)
                elif file_format.lower() == "csv":
                    merged_dataset.to_csv(output_path, index=False)
                else:
                    raise ValueError("Invalid file format. Choose 'xlsx' or 'csv'.")

            return merged_dataset
