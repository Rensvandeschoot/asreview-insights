import pandas as pd
from asreview import open_state
from asreview import ASReviewProject
from asreview import ASReviewData
from pathlib import Path
import tempfile

class extract:
    @staticmethod
    def extract_and_output(project_file_name, output_path=None):
        """
        Extract data from ASReview state and merge it with the original dataset.

        Parameters:
        - project_file_name: Path to the ASReview project file.
        - output_path: Optional output path to save the merged dataset as CSV.

        Returns:
        - merged_dataset: Merged pandas DataFrame containing original dataset and state information.
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
                df_state = state.get_dataset()
                
                # Add labeling order from the index
                df_state["labeling_order"] = df_state.index  

                # Get ranking and probabilities from the last iteration
                last_ranking = state.get_last_ranking()
                last_probabilities = state.get_last_probabilities().to_frame(name='last_probabilities')

                # Drop duplicate columns from df_state to avoid conflicts
                df_state = df_state.drop(columns=['notes'], errors='ignore')

            # Merge state information into the original dataset
            merged_dataset = dataset_df.join(df_state.set_index("record_id"), on="record_id", how="left")
            merged_dataset = merged_dataset.join(last_probabilities, on="record_id", how="left")

            print(f"The state contains {len(df_state)} records.")
            print(merged_dataset.head())
            print(f"The merged dataset contains {len(merged_dataset)} records.")

            # Save as Excel
            if output_path:
                merged_dataset.to_excel(output_path, index=False)

            return merged_dataset
