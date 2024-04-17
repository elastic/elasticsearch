/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc

import spock.lang.Specification
import spock.lang.TempDir

import org.gradle.api.InvalidUserDataException
import org.gradle.testfixtures.ProjectBuilder

import static org.elasticsearch.gradle.internal.doc.RestTestsFromDocSnippetTask.replaceBlockQuote
import static org.elasticsearch.gradle.internal.doc.RestTestsFromDocSnippetTask.shouldAddShardFailureCheck
import static org.elasticsearch.gradle.internal.test.TestUtils.normalizeString

class RestTestsFromDocSnippetTaskSpec extends Specification {

    @TempDir
    File tempDir;

    def "test simple block quote"() {
        expect:
        replaceBlockQuote("\"foo\": \"\"\"bort baz\"\"\"") == "\"foo\": \"bort baz\""
    }

    def "test multiple block quotes"() {
        expect:
        replaceBlockQuote("\"foo\": \"\"\"bort baz\"\"\", \"bar\": \"\"\"other\"\"\"") == "\"foo\": \"bort baz\", \"bar\": \"other\""
    }

    def "test escaping in block quote"() {
        expect:
        replaceBlockQuote("\"foo\": \"\"\"bort\" baz\"\"\"") == "\"foo\": \"bort\\\" baz\""
        replaceBlockQuote("\"foo\": \"\"\"bort\n baz\"\"\"") == "\"foo\": \"bort\\n baz\""
    }

    def "test invalid block quotes"() {
        given:
        String input = "\"foo\": \"\"\"bar\"";
        when:
        RestTestsFromDocSnippetTask.replaceBlockQuote(input);
        then:
        def e = thrown(InvalidUserDataException)
        e.message == "Invalid block quote starting at 7 in:\n" + input
    }

    def "test is doc write request"() {
        expect:
        shouldAddShardFailureCheck("doc-index/_search") == true
        shouldAddShardFailureCheck("_cat") == false
        shouldAddShardFailureCheck("_ml/datafeeds/datafeed-id/_preview") == false
    }

    def "can create rest tests from docs"() {
        def build = ProjectBuilder.builder().build()
        given:
        def task = build.tasks.create("restTestFromSnippet", RestTestsFromDocSnippetTask)
        task.expectedUnconvertedCandidates = ["ml-update-snapshot.asciidoc", "reference/security/authorization/run-as-privilege.asciidoc"]
        docs()
        task.docs = build.fileTree(new File(tempDir, "docs"))
        task.testRoot.convention(build.getLayout().buildDirectory.dir("rest-tests"));

        when:
        task.getActions().forEach { it.execute(task) }
        def restSpec = new File(task.getTestRoot().get().getAsFile(), "rest-api-spec/test/painless-debugging.yml")

        then:
        restSpec.exists()
        normalizeString(restSpec.text, tempDir) == """---
"line_22":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: PUT
        path: "hockey/_doc/1"
        refresh: ""
        body: |
          {"first":"johnny","last":"gaudreau","goals":[9,27,1],"assists":[17,46,0],"gp":[26,82,1]}
  - is_false: _shards.failures
  - do:
      catch: /painless_explain_error/
      raw:
        method: POST
        path: "hockey/_explain/1"
        error_trace: "false"
        body: |
          {
            "query": {
              "script": {
                "script": "Debug.explain(doc.goals)"
              }
            }
          }
  - is_false: _shards.failures
  - match:
      \$body:
        {
           "error": {
              "type": "script_exception",
              "to_string": "[1, 9, 27]",
              "painless_class": "org.elasticsearch.index.fielddata.ScriptDocValues.Longs",
              "java_class": "org.elasticsearch.index.fielddata.ScriptDocValues\$Longs",
              "script_stack": \$body.error.script_stack, "script": \$body.error.script, "lang": \$body.error.lang, "position": \$body.error.position, "caused_by": \$body.error.caused_by, "root_cause": \$body.error.root_cause, "reason": \$body.error.reason
           },
           "status": 400
        }
  - do:
      catch: /painless_explain_error/
      raw:
        method: POST
        path: "hockey/_update/1"
        error_trace: "false"
        body: |
          {
            "script": "Debug.explain(ctx._source)"
          }
  - is_false: _shards.failures
  - match:
      \$body:
        {
          "error" : {
            "root_cause": \$body.error.root_cause,
            "type": "illegal_argument_exception",
            "reason": "failed to execute script",
            "caused_by": {
              "type": "script_exception",
              "to_string": \$body.error.caused_by.to_string,
              "painless_class": "java.util.LinkedHashMap",
              "java_class": "java.util.LinkedHashMap",
              "script_stack": \$body.error.caused_by.script_stack, "script": \$body.error.caused_by.script, "lang": \$body.error.caused_by.lang, "position": \$body.error.caused_by.position, "caused_by": \$body.error.caused_by.caused_by, "reason": \$body.error.caused_by.reason
            }
          },
          "status": 400
        }"""
        def restSpec2 = new File(task.testRoot.get().getAsFile(), "rest-api-spec/test/ml-update-snapshot.yml")
        restSpec2.exists()
        normalizeString(restSpec2.text, tempDir) == """---
"line_50":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
        - always_skip
      reason: todo
  - do:
      raw:
        method: POST
        path: "_ml/anomaly_detectors/it_ops_new_logs/model_snapshots/1491852978/_update"
        body: |
          {
            "description": "Snapshot 1",
            "retain": true
          }
  - is_false: _shards.failures"""
        def restSpec3 = new File(task.testRoot.get().getAsFile(), "rest-api-spec/test/reference/sql/getting-started.yml")
        restSpec3.exists()
        normalizeString(restSpec3.text, tempDir) == """---
"line_10":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: PUT
        path: "library/_bulk"
        refresh: ""
        body: |
          {"index":{"_id": "Leviathan Wakes"}}
          {"name": "Leviathan Wakes", "author": "James S.A. Corey", "release_date": "2011-06-02", "page_count": 561}
          {"index":{"_id": "Hyperion"}}
          {"name": "Hyperion", "author": "Dan Simmons", "release_date": "1989-05-26", "page_count": 482}
          {"index":{"_id": "Dune"}}
          {"name": "Dune", "author": "Frank Herbert", "release_date": "1965-06-01", "page_count": 604}
  - is_false: _shards.failures
  - do:
      raw:
        method: POST
        path: "_sql"
        format: "txt"
        body: |
          {
            "query": "SELECT * FROM library WHERE release_date < '2000-01-01'"
          }
  - is_false: _shards.failures
  - match:
      \$body:
        /    /s+author     /s+/|     /s+name      /s+/|  /s+page_count   /s+/| /s+release_date/s*
         ---------------/+---------------/+---------------/+------------------------/s*
         Dan /s+Simmons    /s+/|Hyperion       /s+/|482            /s+/|1989-05-26T00:00:00.000Z/s*
         Frank /s+Herbert  /s+/|Dune           /s+/|604            /s+/|1965-06-01T00:00:00.000Z/s*/"""
    def restSpec4 = new File(task.testRoot.get().getAsFile(), "rest-api-spec/test/reference/security/authorization/run-as-privilege.yml")
    restSpec4.exists()
    normalizeString(restSpec4.text, tempDir) == """---
"line_51":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/role/my_director"
        refresh: "true"
        body: |
          {
            "cluster": ["manage"],
            "indices": [
              {
                "names": [ "index1", "index2" ],
                "privileges": [ "manage" ]
              }
            ],
            "run_as": [ "jacknich", "rdeniro" ],
            "metadata" : {
              "version" : 1
            }
          }
  - is_false: _shards.failures
---
"line_114":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/role/my_admin_role"
        refresh: "true"
        body: |
          {
            "cluster": ["manage"],
            "indices": [
              {
                "names": [ "index1", "index2" ],
                "privileges": [ "manage" ]
              }
            ],
            "applications": [
              {
                "application": "myapp",
                "privileges": [ "admin", "read" ],
                "resources": [ "*" ]
              }
            ],
            "run_as": [ "analyst_user" ],
            "metadata" : {
              "version" : 1
            }
          }
  - is_false: _shards.failures
---
"line_143":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/role/my_analyst_role"
        refresh: "true"
        body: |
          {
            "cluster": [ "monitor"],
            "indices": [
              {
                "names": [ "index1", "index2" ],
                "privileges": ["manage"]
              }
            ],
            "applications": [
              {
                "application": "myapp",
                "privileges": [ "read" ],
                "resources": [ "*" ]
              }
            ],
            "metadata" : {
              "version" : 1
            }
          }
  - is_false: _shards.failures
---
"line_170":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/user/admin_user"
        refresh: "true"
        body: |
          {
            "password": "l0ng-r4nd0m-p@ssw0rd",
            "roles": [ "my_admin_role" ],
            "full_name": "Eirian Zola",
            "metadata": { "intelligence" : 7}
          }
  - is_false: _shards.failures
---
"line_184":
  - skip:
      features:
        - default_shards
        - stash_in_key
        - stash_in_path
        - stash_path_replace
        - warnings
  - do:
      raw:
        method: POST
        path: "_security/user/analyst_user"
        refresh: "true"
        body: |
          {
            "password": "l0nger-r4nd0mer-p@ssw0rd",
            "roles": [ "my_analyst_role" ],
            "full_name": "Monday Jaffe",
            "metadata": { "innovation" : 8}
          }
  - is_false: _shards.failures"""
}

    File docFile(String fileName, String docContent) {
        def file = tempDir.toPath().resolve(fileName).toFile()
        file.parentFile.mkdirs()
        file.text = docContent
        return file
    }


    void docs() {
        docFile(
            "docs/reference/sql/getting-started.asciidoc", """
[role="xpack"]
[[sql-getting-started]]
== Getting Started with SQL

To start using {es-sql}, create
an index with some data to experiment with:

[source,console]
--------------------------------------------------
PUT /library/_bulk?refresh
{"index":{"_id": "Leviathan Wakes"}}
{"name": "Leviathan Wakes", "author": "James S.A. Corey", "release_date": "2011-06-02", "page_count": 561}
{"index":{"_id": "Hyperion"}}
{"name": "Hyperion", "author": "Dan Simmons", "release_date": "1989-05-26", "page_count": 482}
{"index":{"_id": "Dune"}}
{"name": "Dune", "author": "Frank Herbert", "release_date": "1965-06-01", "page_count": 604}
--------------------------------------------------

And now you can execute SQL using the <<sql-search-api,SQL search API>>:

[source,console]
--------------------------------------------------
POST /_sql?format=txt
{
  "query": "SELECT * FROM library WHERE release_date < '2000-01-01'"
}
--------------------------------------------------
// TEST[continued]

Which should return something along the lines of:

[source,text]
--------------------------------------------------
    author     |     name      |  page_count   | release_date
---------------+---------------+---------------+------------------------
Dan Simmons    |Hyperion       |482            |1989-05-26T00:00:00.000Z
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z
--------------------------------------------------
// TESTRESPONSE[s/\\|/\\\\|/ s/\\+/\\\\+/]
// TESTRESPONSE[non_json]

You can also use the <<sql-cli>>. There is a script to start it
shipped in x-pack's bin directory:

[source,bash]
--------------------------------------------------
\$ ./bin/elasticsearch-sql-cli
--------------------------------------------------

From there you can run the same query:

[source,sqlcli]
--------------------------------------------------
sql> SELECT * FROM library WHERE release_date < '2000-01-01';
    author     |     name      |  page_count   | release_date
---------------+---------------+---------------+------------------------
Dan Simmons    |Hyperion       |482            |1989-05-26T00:00:00.000Z
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z
--------------------------------------------------
"""
        )
        docFile(
            "docs/ml-update-snapshot.asciidoc",
            """
[role="xpack"]
[[ml-update-snapshot]]
= Update model snapshots API
++++
<titleabbrev>Update model snapshots</titleabbrev>
++++

Updates certain properties of a snapshot.

[[ml-update-snapshot-request]]
== {api-request-title}

`POST _ml/anomaly_detectors/<job_id>/model_snapshots/<snapshot_id>/_update`

[[ml-update-snapshot-prereqs]]
== {api-prereq-title}

Requires the `manage_ml` cluster privilege. This privilege is included in the
`machine_learning_admin` built-in role.

[[ml-update-snapshot-path-parms]]
== {api-path-parms-title}

`<job_id>`::
(Required, string)
include::{es-ref-dir}/ml/ml-shared.asciidoc[tag=job-id-anomaly-detection]

`<snapshot_id>`::
(Required, string)
include::{es-ref-dir}/ml/ml-shared.asciidoc[tag=snapshot-id]

[[ml-update-snapshot-request-body]]
== {api-request-body-title}

The following properties can be updated after the model snapshot is created:

`description`::
(Optional, string) A description of the model snapshot.

`retain`::
(Optional, Boolean)
include::{es-ref-dir}/ml/ml-shared.asciidoc[tag=retain]


[[ml-update-snapshot-example]]
== {api-examples-title}

[source,console]
--------------------------------------------------
POST
_ml/anomaly_detectors/it_ops_new_logs/model_snapshots/1491852978/_update
{
  "description": "Snapshot 1",
  "retain": true
}
--------------------------------------------------
// TEST[skip:todo]

When the snapshot is updated, you receive the following results:
[source,js]
----
{
  "acknowledged": true,
  "model": {
    "job_id": "it_ops_new_logs",
    "timestamp": 1491852978000,
    "description": "Snapshot 1",
...
    "retain": true
  }
}
----

"""
        )

        docFile(
            "docs/painless-debugging.asciidoc",
            """

[[painless-debugging]]
=== Painless Debugging

==== Debug.Explain

Painless doesn't have a
{wikipedia}/Read%E2%80%93eval%E2%80%93print_loop[REPL]
and while it'd be nice for it to have one day, it wouldn't tell you the
whole story around debugging painless scripts embedded in Elasticsearch because
the data that the scripts have access to or "context" is so important. For now
the best way to debug embedded scripts is by throwing exceptions at choice
places. While you can throw your own exceptions
(`throw new Exception('whatever')`), Painless's sandbox prevents you from
accessing useful information like the type of an object. So Painless has a
utility method, `Debug.explain` which throws the exception for you. For
example, you can use {ref}/search-explain.html[`_explain`] to explore the
context available to a {ref}/query-dsl-script-query.html[script query].

[source,console]
---------------------------------------------------------
PUT /hockey/_doc/1?refresh
{"first":"johnny","last":"gaudreau","goals":[9,27,1],"assists":[17,46,0],"gp":[26,82,1]}

POST /hockey/_explain/1
{
  "query": {
    "script": {
      "script": "Debug.explain(doc.goals)"
    }
  }
}
---------------------------------------------------------
// TEST[s/_explain\\/1/_explain\\/1?error_trace=false/ catch:/painless_explain_error/]
// The test system sends error_trace=true by default for easier debugging so
// we have to override it to get a normal shaped response

Which shows that the class of `doc.first` is
`org.elasticsearch.index.fielddata.ScriptDocValues.Longs` by responding with:

[source,console-result]
---------------------------------------------------------
{
   "error": {
      "type": "script_exception",
      "to_string": "[1, 9, 27]",
      "painless_class": "org.elasticsearch.index.fielddata.ScriptDocValues.Longs",
      "java_class": "org.elasticsearch.index.fielddata.ScriptDocValues\$Longs",
      ...
   },
   "status": 400
}
---------------------------------------------------------
// TESTRESPONSE[s/\\.\\.\\./"script_stack": \$body.error.script_stack, "script": \$body.error.script, "lang": \$body.error.lang, "position": \$body.error.position, "caused_by": \$body.error.caused_by, "root_cause": \$body.error.root_cause, "reason": \$body.error.reason/]

You can use the same trick to see that `_source` is a `LinkedHashMap`
in the `_update` API:

[source,console]
---------------------------------------------------------
POST /hockey/_update/1
{
  "script": "Debug.explain(ctx._source)"
}
---------------------------------------------------------
// TEST[continued s/_update\\/1/_update\\/1?error_trace=false/ catch:/painless_explain_error/]

The response looks like:

[source,console-result]
---------------------------------------------------------
{
  "error" : {
    "root_cause": ...,
    "type": "illegal_argument_exception",
    "reason": "failed to execute script",
    "caused_by": {
      "type": "script_exception",
      "to_string": "{gp=[26, 82, 1], last=gaudreau, assists=[17, 46, 0], first=johnny, goals=[9, 27, 1]}",
      "painless_class": "java.util.LinkedHashMap",
      "java_class": "java.util.LinkedHashMap",
      ...
    }
  },
  "status": 400
}
---------------------------------------------------------
// TESTRESPONSE[s/"root_cause": \\.\\.\\./"root_cause": \$body.error.root_cause/]
// TESTRESPONSE[s/\\.\\.\\./"script_stack": \$body.error.caused_by.script_stack, "script": \$body.error.caused_by.script, "lang": \$body.error.caused_by.lang, "position": \$body.error.caused_by.position, "caused_by": \$body.error.caused_by.caused_by, "reason": \$body.error.caused_by.reason/]
// TESTRESPONSE[s/"to_string": ".+"/"to_string": \$body.error.caused_by.to_string/]

Once you have a class you can go to <<painless-api-reference>> to see a list of
available methods.

"""
        )
        docFile(
            "docs/reference/security/authorization/run-as-privilege.asciidoc",
            """[role="xpack"]
[[run-as-privilege]]
= Submitting requests on behalf of other users

{es} roles support a `run_as` privilege that enables an authenticated user to
submit requests on behalf of other users. For example, if your external
application is trusted to authenticate users, {es} can authenticate the external
application and use the _run as_ mechanism to issue authorized requests as
other users without having to re-authenticate each user.

To "run as" (impersonate) another user, the first user (the authenticating user)
must be authenticated by a mechanism that supports run-as delegation. The second
user (the `run_as` user) must be authorized by a mechanism that supports
delegated run-as lookups by username.

The `run_as` privilege essentially operates like a secondary form of
<<authorization_realms,delegated authorization>>. Delegated authorization applies
to the authenticating user, and the `run_as` privilege applies to the user who
is being impersonated.

Authenticating user::
--
For the authenticating user, the following realms (plus API keys) all support
`run_as` delegation: `native`, `file`, Active Directory, JWT, Kerberos, LDAP and
PKI.

Service tokens, the {es} Token Service, SAML 2.0, and OIDC 1.0 do not
support `run_as` delegation.
--

`run_as` user::
--
{es} supports `run_as` for any realm that supports user lookup.
Not all realms support user lookup. Refer to the list of <<user-lookup,supported realms>>
and ensure that the realm you wish to use is configured in a manner that
supports user lookup.

The `run_as` user must be retrieved from a <<realms,realm>> - it is not
possible to run as a
<<service-accounts,service account>>,
<<token-authentication-api-key,API key>> or
<<token-authentication-access-token,access token>>.
--

To submit requests on behalf of other users, you need to have the `run_as`
privilege in your <<defining-roles,roles>>. For example, the following request
creates a `my_director` role that grants permission to submit request on behalf
of `jacknich` or `redeniro`:

[source,console]
----
POST /_security/role/my_director?refresh=true
{
  "cluster": ["manage"],
  "indices": [
    {
      "names": [ "index1", "index2" ],
      "privileges": [ "manage" ]
    }
  ],
  "run_as": [ "jacknich", "rdeniro" ],
  "metadata" : {
    "version" : 1
  }
}
----

To submit a request as another user, you specify the user in the
`es-security-runas-user` request header. For example:

[source,sh]
----
curl -H "es-security-runas-user: jacknich" -u es-admin -X GET http://localhost:9200/
----

The `run_as` user passed in through the `es-security-runas-user` header must be
available from a realm that supports delegated authorization lookup by username.
Realms that don't support user lookup can't be used by `run_as` delegation from
other realms.

For example, JWT realms can authenticate external users specified in JWTs, and
execute requests as a `run_as` user in the `native` realm. {es} will retrieve the
indicated `runas` user and execute the request as that user using their roles.

[[run-as-privilege-apply]]
== Apply the `run_as` privilege to roles
You can apply the `run_as` privilege when creating roles with the
<<security-api-put-role,create or update roles API>>. Users who are assigned
a role that contains the `run_as` privilege inherit all privileges from their
role, and can also submit requests on behalf of the indicated users.

NOTE: Roles for the authenticated user and the `run_as` user are not merged. If
a user authenticates without specifying the `run_as` parameter, only the
authenticated user's roles are used. If a user authenticates and their roles
include the `run_as` parameter, only the `run_as` user's roles are used.

After a user successfully authenticates to {es}, an authorization process determines whether the user behind an incoming request is allowed to run
that request. If the authenticated user has the `run_as` privilege in their list
of permissions and specifies the run-as header, {es} _discards_ the authenticated
user and associated roles. It then looks in each of the configured realms in the
realm chain until it finds the username that's associated with the `run_as` user,
and uses those roles to execute any requests.

Consider an admin role and an analyst role. The admin role has higher privileges,
but might also want to submit requests as another user to test and verify their
permissions.

First, we'll create an admin role named `my_admin_role`. This role has `manage`
<<security-privileges,privileges>> on the entire cluster, and on a subset of
indices. This role also contains the `run_as` privilege, which enables any user
with this role to submit requests on behalf of the specified `analyst_user`.

[source,console]
----
POST /_security/role/my_admin_role?refresh=true
{
  "cluster": ["manage"],
  "indices": [
    {
      "names": [ "index1", "index2" ],
      "privileges": [ "manage" ]
    }
  ],
  "applications": [
    {
      "application": "myapp",
      "privileges": [ "admin", "read" ],
      "resources": [ "*" ]
    }
  ],
  "run_as": [ "analyst_user" ],
  "metadata" : {
    "version" : 1
  }
}
----

Next, we'll create an analyst role named `my_analyst_role`, which has more
restricted `monitor` cluster privileges and `manage` privileges on a subset of
indices.

[source,console]
----
POST /_security/role/my_analyst_role?refresh=true
{
  "cluster": [ "monitor"],
  "indices": [
    {
      "names": [ "index1", "index2" ],
      "privileges": ["manage"]
    }
  ],
  "applications": [
    {
      "application": "myapp",
      "privileges": [ "read" ],
      "resources": [ "*" ]
    }
  ],
  "metadata" : {
    "version" : 1
  }
}
----

We'll create an administrator user and assign them the role named `my_admin_role`,
which allows this user to submit requests as the `analyst_user`.

[source,console]
----
POST /_security/user/admin_user?refresh=true
{
  "password": "l0ng-r4nd0m-p@ssw0rd",
  "roles": [ "my_admin_role" ],
  "full_name": "Eirian Zola",
  "metadata": { "intelligence" : 7}
}
----

We can also create an analyst user and assign them the role named
`my_analyst_role`.

[source,console]
----
POST /_security/user/analyst_user?refresh=true
{
  "password": "l0nger-r4nd0mer-p@ssw0rd",
  "roles": [ "my_analyst_role" ],
  "full_name": "Monday Jaffe",
  "metadata": { "innovation" : 8}
}
----

You can then authenticate to {es} as the `admin_user` or `analyst_user`. However, the `admin_user` could optionally submit requests on
behalf of the `analyst_user`. The following request authenticates to {es} with a
`Basic` authorization token and submits the request as the `analyst_user`:

[source,sh]
----
curl -s -X GET -H "Authorization: Basic YWRtaW5fdXNlcjpsMG5nLXI0bmQwbS1wQHNzdzByZA==" -H "es-security-runas-user: analyst_user" https://localhost:9200/_security/_authenticate
----

The response indicates that the `analyst_user` submitted this request, using the
`my_analyst_role` that's assigned to that user. When the `admin_user` submitted
the request, {es} authenticated that user, discarded their roles, and then used
the roles of the `run_as` user.

[source,sh]
----
{"username":"analyst_user","roles":["my_analyst_role"],"full_name":"Monday Jaffe","email":null,
"metadata":{"innovation":8},"enabled":true,"authentication_realm":{"name":"native",
"type":"native"},"lookup_realm":{"name":"native","type":"native"},"authentication_type":"realm"}
%
----

The `authentication_realm` and `lookup_realm` in the response both specify
the `native` realm because both the `admin_user` and `analyst_user` are from
that realm. If the two users are in different realms, the values for
`authentication_realm` and `lookup_realm` are different (such as `pki` and
`native`).
"""
        )

    }
}
