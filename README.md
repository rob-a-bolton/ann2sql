# ann2sql

## What is this?

A quick script to load [MedCAT](https://github.com/CogStack/MedCAT/) annotations into a database via JDBC.  
This exists as limitations of [RML](https://rml.io/) and [JSON Path](https://datatracker.ietf.org/wg/jsonpath/about/) intersect to cause an issue for importing certain data.  

This was created specifically for working with MIMIC-III data.  

## Usage

`clj -m ann2sql.core --jdbc 'jdbc:postgresql://localhost/annotations?user=myuser&password=mypass' import --dir ./some/data-dir/`  
Any JDBC string may be used but only the postgres and sqlite drivers have been included in the project deps.  
The dir option specifies where to find the files.  
Expected format of files is that each file stores the annotations for a single document.  
The application walks the filesystem path recursively so the files may be in any hierarchy under `dir` but ensure that no other files exist there other than the correctly formatted JSON annotation files.

Please note that any user may read your process' command line args. Connecting to a database with this application *WILL* leak your DB username/password to any user/process on the system that's looking for it.  
This may be fixed in the future but the app is currently run on a single-user PC against a local DB for the purpose of research.  
Should anybody wish to use the script and they have concerns about, submit an issue and it shall be fixed.

Necessary fields:
```json
{
  "hadm_id": "<admission ID>",
  "row_id": "<noteevents row ID>",
  "annotations": [
    {
      // fields output by MedCATService
      "tui": "<tui>",
      "cui": "<cui>",
      "start": "<char offset substring start>",
      "end": "<char offset substring end>",
      "acc": "<accuracy, a double>",
      "source_value": "<substring matched/extracted>",
      "meta_anns": {
        "status": {
          "name": "Status",
          "value": "<status value>"
        }
      }
    }
  ]
}
```


## Why

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
