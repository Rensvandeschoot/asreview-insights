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
            dataset_df = dataset.to_dataframe()  # Convert ASReviewData to DataFrame
            dataset_df['record_id'] = dataset.record_ids  # Ensure record_id is a column

            # Access the review data from the state file
            with open_state(project_file_name) as state:
                review_data = state.get_dataset()
                review_data['record_id'] = review_data.index  # Convert index to column
                last_probabilities = state.get_last_probabilities()
                last_probabilities_df = last_probabilities.to_frame(name='last_probabilities')
                last_probabilities_df['record_id'] = last_probabilities_df.index  # Convert index to column

            # Merge original dataset with review data using record_id
            merged_dataset = dataset_df.merge(review_data, on='record_id', how='left', suffixes=('_original', '_review'))

            # Merge last probabilities with the merged dataset
            merged_dataset = merged_dataset.merge(last_probabilities_df, on='record_id', how='left')

            print(merged_dataset.head())
            print(f"The merged dataset contains {len(merged_dataset)} records.")

            # Save as Excel
            if output_path:
                merged_dataset.to_excel(output_path, index=False)

            return merged_dataset
