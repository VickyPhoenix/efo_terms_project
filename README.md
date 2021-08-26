# efo_terms_project
Retrieve specific data provided by the Ontology Lookup Service repository https://www.ebi.ac.uk/ols/index

For this project i used pandas 0.25 and python 3.5.8. Windows 10 OS.

My modeling uses a main fact table "efo_terms" where the efo_id column should be the key. The dimension tables are: efo_synonyms and efo_relations with efo_id as foreign key.
I assumed that the EFO term ontology (parent links) are the links mentioned as parent/children. For the synonyms, the assumption is that those fields are retrieved from "synonyms" key. 
Efo_terms fact table consists of primary key (efo_id) and label, iri and TimeStamp.
Efo_synonyms table consists of foreign key (efo_id) synonyms and TimeStamp.
Efo_relations table consists of foreign key (efo_id), parents, children and TimeStamp.
TimeStamp column exists in order to monitor new rows or updated rows.

In order to proceed in execution, I installed on my computer PostgreSQL using the below link:
https://www.postgresqltutorial.com/install-postgresql/.

The .py file's place is up to you. In order to run:
1. Open cmd or PowerShell 
2. Go to the .py directory
3. Execute: python efo_terms_assignment.py --user @your_user_name --post_pass @your_password

Arguments are mandatory to provide.
