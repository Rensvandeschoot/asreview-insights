import pandas as pd
from asreview import ASReviewProject
from asreview import open_state
from pathlib import Path
import tempfile
import json

class read:
    @staticmethod
    def read_and_output(project_file_name, output_path=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_project_path = Path(temp_dir)
            project = ASReviewProject.load(project_file_name, temp_project_path)
            project_details = project.config

            # Extract settings metadata
            with open_state(project_file_name) as state:
                settings = state.settings_metadata

            # Prepare data for DataFrame and printing
            formatted_data = []
            for key, value in {**project_details, **settings}.items():
                formatted_value = json.dumps(value, indent=4) if isinstance(value, (dict, list)) else value
                formatted_data.append((key, formatted_value))
                print(f"{key}: {formatted_value}")

            # Convert to DataFrame in long format
            project_df = pd.DataFrame(formatted_data, columns=['Key', 'Value'])

            # Save to CSV or Excel if output path is provided
            if output_path:
                if output_path.endswith('.xlsx'):
                    project_df.to_excel(output_path, index=False)
                else:
                    project_df.to_csv(output_path, index=False)
