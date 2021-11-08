# ann2sql

## What is this?

A quick script to use [MedCATservice](https://github.com/CogStack/MedCATservice) to annotate documents store in a SQL database.  
This exists as limitations of [RML](https://rml.io/) and [JSON Path](https://datatracker.ietf.org/wg/jsonpath/about/) intersect to cause an issue for importing certain data.  
For this reason it is necessary store the annotations in a database rather than attempt to use RML on the annotation JSON returned from MedCATservice.  

This should work for any dataset but has only been tested against MIMIC-III and the [n2c2 Track 1: Cohort Selection for Clinical Trials](https://portal.dbmi.hms.harvard.edu/projects/n2c2-2018-t1/) data.

## Usage

The following example shows how to annotate documents stored in a Postgres database and output to a sqlite db:  
`clj -M -m ann2sql.core -s 'jdbc:postgresql://localhost/mimic?user=username&password=password' -d 'jdbc:sqlite:output.db' -S train -D train_out -c pat_id -c doc_id -t text --drop-tables --create-tables`  

This would run across the rows of an input table like this:
| pat_id | doc_id |             text |
|--------|--------|------------------|
|      0 |      0 | "ankle improved" |
|      0 |      1 |     "ambulation" |

and produce an output table like this:
| pat_id | doc_id | seq_id |      cui | source_value | m_start | m_end |          acc |
|--------|--------|--------|----------|--------------|---------|-------|--------------|
|      0 |      0 |      0 | C0003086 |      "ankle" |       0 |     6 |         0.99 |
|      0 |      0 |      1 | C4321457 |   "improved" |       7 |    15 | 0.2744009742 |
|      0 |      1 |      0 | C0080331 | "ambulation" |       0 |    10 |         0.99 |

Command-line options:

+ **-u**, **--medcat-url**, URL for MedCATservice. Default: *http://127.0.0.1:5000/api/process_bulk*
+ **-s**, **--src-jdbc**, JDBC connection string for source database
+ **-d**, **--dst-jdbc**, JDBC connection string for destination database
+ **-S**, **--src-table**, Table in source database containing data
+ **-D**, **--dst-table**,  Table to insert annotations to in destination database
+ **-c**, **--src-columns**, Columns in source data to preserve when storing in destination
+ **-t**, **--text-column**,  Column containing text to annotate
+ **-b**, **--src-batch-size**, Number of source documents to annotate at once. Default: 100
+ **-B**, **--dst-batch-size**, Number of annotation results to batch for writing to database. Default: 1000
+ **--drop-tables**, Whether to drop the output annotation table before starting.
+ **--create-tables**, Whether to create the output annotation table before starting (if-not-exists)

> *Note: for --src-jdbc and --dst-dbc, the environment variables SRC_JDBC and DST_JDBC may be used instead*

The script will attempt to guess the right datatype to use for storing a double.
It also attempts to guess how to translate the *--src-columns* fields to the
output database.  
Neither of these actions are perfect. You can avoid both these issues by not
using the *--create-tables* option and ensuring the output table exists before
running the script.
## Why

Ideally a script would capture the JSON output from MedCATservice and use RML to
convert this directly to an RDF graph, but that's not possible.

RML requires an iterator to create resources. It's typical in JSON to nest data such that a "root" object has an array of child objects which represent some associated data e.g.

```json
[
  {
    "name": "annotationDocument",
    "text": "Patient diagnosed with [...]",
    "uniqueId": 1014,
    "annotations": [
      {
        "name": "Patient",
        "accuracy": 1.0,
        "position": 0
      },
      {
        "name": "diagnosis (event)",
        "accuracy": 0.89,
        "position": 8
      },
      // ...
    ]
  },
  {
    "name": "annotationDocument",
    "text": "3.14pm administered 10ml of [...]",
    "uniqueId": 1015,
    "annotations": [
      {
        "name": "administer (procedure)",
        "accuracy": 0.95,
        "position": 7
      },
      // ...
    ]
  }
]

```
Suppose you want to associate each document with structured information contanining the annotations, such that each annotation is a resource with its own triples.  
Achieving this is not possible (to my knowledge, please get in touch if this is incorrect) directly using RML.  

You have two choices of iteration:
* Iterate over the root objects, `$[*]`
* Iterate over the annotations, `$[*].annotations`

If iterating over the root objects then your iterator has access to the `uniqueId`, necessary to attach information to an identifier (via `rr:template`).  
The downside is that you may only then specify triples that use this `uniqueId` as their resource. Each field in an object in `annotations` can be tied directly to your `annotationDocument`, but not to their sibling info.  

This prevents tying e.g. the `accuracy` field of an annotation to its name and position.  
To do this it would be necessary to create a node for each annotation and associate the annotation to the document's node.  
In this case the annotation node may be either a named resource or a blank node.  
In the case of a named resource, it must have some unique information that may be used to name it and uniquely reference it.  
Unfortunately this is not the case - annotations don't have an ID.  
Instead a blank node could be used, but there is still no way to associate it with its document as RML does not provide nested iterators.  

Storing per-annotation data together requires a node, necessitating iteration over the child objects. Iterating over the child objects prevents linking to the parent objects (in the absence of a unique ID).

To get around this, `ann2sql` exists to store annotation data in a SQL database.  
Each row contains an annotation along with the `hadm_id` and `row_id` (admission ID, noteevents document ID).  
