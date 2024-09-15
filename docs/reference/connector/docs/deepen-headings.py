import os
import re

def deepen_subheadings(content):
    """
    Deepen the level of all headings in the content by adding one '=' to each.
    
    Parameters:
        content (str): The content in which to deepen the subheadings.
    
    Returns:
        str: The content with deepened subheadings.
    """

    # Add "=" to all headings
    adjusted_content = re.sub(r'^(=+)( .+)$', r'\1=\2', content, flags=re.MULTILINE)
    return adjusted_content

# List of AsciiDoc files that need heading adjustments
file_list = ["mongodb-start.asciidoc"
            
             
]

def process_files(file_list):
    for file_name in file_list:
        try:
            with open(file_name, 'r', encoding='utf-8') as file:
                content = file.read()
            # Deepen subheadings
            new_content = deepen_subheadings(content)
            # Write the updated content back to the file
            with open(file_name, 'w', encoding='utf-8') as file:
                file.write(new_content)
            print(f"Processed {file_name} successfully.")
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

# Run the function
process_files(file_list)
