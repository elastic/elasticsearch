Versioning Elasticsearch
========================

Elasticsearch is a complicated product and is run in many different scenarios.
A single version number is not sufficient to cover the whole of the product,
instead we need different concepts to provide versioning capabilities
for different aspects of Elasticsearch, depending on their scope, updatability,
responsiveness, and maintenance.

## Release version

This is the version number used for published releases of Elasticsearch,
and the Elastic stack. This takes the form _major.minor.patch_,
with a corresponding version id.

Use of this version number should be avoided, as it does not apply to
some scenarios, and use of a release version will break Elasticsearch nodes.

The release version is accessible in code through `Build.current().version()`,
but it **should not** be assumed that this is a semantic version number,
it could be any arbitrary string.

## Transport protocol

The transport protocol is used to send binary data between Elasticsearch nodes; the
`TransportVersion` is the version number used for this protocol.
This version number is negotiated between each pair of nodes in the cluster
on first connection, and is set as the lower of the highest transport version
understood by each node.
This version is then accessible through the `getTransportVersion` method
on `StreamInput` and `StreamOutput`, so serialization code can read/write
objects in a form that will be understood by the other node.

Internally, every change to the transport protocol is represented by a new transport version, higher than all previous
transport versions, which then becomes the highest version recognized by that build of Elasticsearch. The version ids
are stored
in state files in the `transport/` resources directory.
Each id has a standard pattern `M_NNN_S_PP`, where:

* `M` is the major version
* `NNN` is an incrementing id
* `S` is used in subsidiary repos amending the default transport protocol
* `PP` is used for patches and backports

* For every change to the transport protocol (serialization of any object), we need to create a new transport version
  _in the same PR that adds the change_.

[//]: # (TODO description of state files, patch version, and latest upper bounds)

_Elastic developers_ - please see corresponding documentation for Serverless
on creating transport versions for Serverless changes.

### Creating transport versions

We use gradle tooling to manage the creation of transport versions state files. To create a new transport version, first
declare a reference anywhere in the java code. The `fromName` arg (called the "name") must be a string literal, composed
only of characters that are alphanumeric or underscores:

    private static final TransportVersion MY_NEW_TV = TransportVersion.fromName("my_new_tv"); 

Next, you need to create the transport version state stored in the resource files, including the creation of a new
transport version id. This is done by running the `:generateTransportVersion` gradle task. The generation task can
automatically detect the name by checking if a state file is missing for a given constant. e.g.:

    ./gradlew :generateTransportVersion

Once you have declared your constant and generated its state, you can then use it to gate serialization code. e.g.:

    if (in.getTransportVersion().supports(MY_NEW_TV)) {
        // new serialization code
    }

Note that the generation task is idempotent. This means that if you have already generated a transport version file, and
you run the task again, it will not create a duplicate transport version id. If you remove backports (described below)
from a subsequent run, the backport will be removed from the state files. It is important to note that only one
transport version may be generated for a given PR, though it can be referenced in multiple places in the code by using
the same name.

### Reverting changes

#### On main

Once a transport change with a new version has been merged into main or a release branch,
it **must not** be modified - this is so the meaning of that specific
transport version does not change.

If a transport version change on main needs to be reverted, a **new** version constant
should be added representing the revert. The version id checks should be
adjusted to only use the modified protocol between the version id
the change was added, and the new version id used for the revert (exclusive).

    TransportVersion inTV = in.getTransportVersion();
    if (inTV.supports(MY_ORIGINAL_TV) && inTV.supports(MY_REVERT_TV) == false) {
        // old serialization code
    } else if (inTV.supports(MY_REVERT_TV)) {
        // new serialization code
    }

#### On a PR

A transport version change in a PR that has not yet been merged can be easily reverted by simply removing the code that
references the transport version (`TransportVersion.fromName("my_new_tv");`), then rerunning the generation task with no
args. Because this command is idempotent, it will detect that the transport version constant is no longer referenced,
and remove the corresponding state file. E.g.:

    ./gradlew :generateTransportVersion

### Backporting transport versions

To prevent race conditions, all transport version ids are first created on the `main` branch, after which they can be
backported to prior release branches.

Backport state files can either be generated on the command line by gradle, or by adding a gihub version label to your
PR.

If you have already generated a transport version file, gradle will not be able to automatically detect which transport
version you want to create backport ids for, so you will need to specify the name as an arg. E.g.:

    ./gradlew :generateTransportVersion --name=my_new_tv --backport-branches=9.1,9.0

If you later decide that you'd like to remove a backport, you can rerun the above command without the branch you want to
remove. e.g. to remove the 9.0 backport:

    ./gradlew :generateTransportVersion --name=my_new_tv --backport-branches=9.1

You can also remove the label on github, and this will trigger CI to remove the backport.

It is also safe to create backports for pre-existing transport versions on main, just be sure to include the code
changes in the same PR to the prior release branch (these must be part of a single atomic commit). E.g.:

    ./gradlew :generateTransportVersion --name=my_preexisting_tv --backport-branches=9.1,9.0

#### Merge conflicts

By design, a merge conflict will occur when two different PRs try to create the same transport version id (e.g. because
they were both based off the same prior id in main). This will produce a merge conflict in a state file in the
`transport/upper_bounds/` resource directory. To resolve the conflict, rerun the full generation command, and it will
regenerate the state files, including the new transport version id. E.g.:

    ./gradlew :generateTransportVersion --name=my_new_tv 

### Minimum compatibility versions

The transport version used between two nodes is determined by the initial handshake
(see `TransportHandshaker`, where the two nodes swap their highest known transport version).
The lowest transport version that is compatible with the current node
is determined by `TransportVersions.MINIMUM_COMPATIBLE`,
and the node is prevented from joining the cluster if it is below that version.
This constant should be updated manually on a major release.

The minimum version that can be used for CCS is determined by
`TransportVersions.MINIMUM_CCS_VERSION`, but this is not actively checked
before queries are performed. Only if a query cannot be serialized at that
version is an action rejected. This constant is updated automatically
as part of performing a release.

### Mapping to release versions

For releases that do use a version number, it can be confusing to encounter
a log or exception message that references an arbitrary transport version,
where you don't know which release version that corresponds to. This is where
the `.toReleaseVersion()` method comes in. It uses metadata stored in a csv file
(`TransportVersions.csv`) to map from the transport version id to the corresponding
release version. For any transport versions it encounters without a direct map,
it performs a best guess based on the information it has. The csv file
is updated automatically as part of performing a release.

In releases that do not have a release version number, that method becomes
a no-op.

## Index version

Index version is a single incrementing version number for the index data format,
metadata, and associated mappings. It is declared the same way as the
transport version - with the pattern `M_NNN_S_PP`, for the major version, version id,
subsidiary version id, and patch number respectively.

Index version is stored in index metadata when an index is created,
and it is used to determine the storage format and what functionality that index supports.
The index version does not change once an index is created.

In the same way as transport versions, when a change is needed to the index
data format or metadata, or new mapping types are added, create a new version constant
below the last one, incrementing the `NNN` version component.

Unlike transport version, version constants cannot be collapsed together,
as an index keeps its creation version id once it is created.
Fortunately, new index versions are only created once a month or so,
so we don’t have a large list of index versions that need managing.

Similar to transport version, index version has a `toReleaseVersion` to map
onto release versions, in appropriate situations.

## Cluster Features

Cluster features are identifiers, published by a node in cluster state,
indicating they support a particular top-level operation or set of functionality.
They are used for internal checks within Elasticsearch, and for gating tests
on certain functionality. For example, to check all nodes have upgraded
to a certain point before running a large migration operation to a new data format.
Cluster features should not be referenced by anything outside the Elasticsearch codebase.

Cluster features are indicative of top-level functionality introduced to
Elasticsearch - e.g. a new transport endpoint, or new operations.

It is also used to check nodes can join a cluster - once all nodes in a cluster
support a particular feature, no nodes can then join the cluster that do not
support that feature. This is to ensure that once a feature is supported
by a cluster, it will then always be supported in the future.

To declare a new cluster feature, add an implementation of the `FeatureSpecification` SPI,
suitably registered (or use an existing one for your code area), and add the feature
as a constant to be returned by getFeatures. To then check whether all nodes
in the cluster support that feature, use the method `clusterHasFeature` on `FeatureService`.
It is only possible to check whether all nodes in the cluster have a feature;
individual node checks should not be done.

Once a cluster feature is declared and deployed, it cannot be modified or removed,
else new nodes will not be able to join existing clusters.
If functionality represented by a cluster feature needs to be removed,
a new cluster feature should be added indicating that functionality is no longer
supported, and the code modified accordingly (bearing in mind additional BwC constraints).

The cluster features infrastructure is only designed to support a few hundred features
per major release, and once features are added to a cluster they can not be removed.
Cluster features should therefore be used sparingly.
Adding too many cluster features risks increasing cluster instability.

When we release a new major version N, we limit our backwards compatibility
to the highest minor of the previous major N-1. Therefore, any cluster formed
with the new major version is guaranteed to have all features introduced during
releases of major N-1. All such features can be deemed to be met by the cluster,
and the features themselves can be removed from cluster state over time,
and the feature checks removed from the code of major version N.

### Testing

Tests often want to check if a certain feature is implemented / available on all nodes,
particularly BwC or mixed cluster test.

Rather than introducing a production feature just for a test condition,
this can be done by adding a _test feature_ in an implementation of
`FeatureSpecification.getTestFeatures`. These features will only be set
on clusters running as part of an integration test. Even so, cluster features
should be used sparingly if possible; Capabilities is generally a better
option for test conditions.

In Java Rest tests, checking cluster features can be done using
`ESRestTestCase.clusterHasFeature(feature)`

In YAML Rest tests, conditions can be defined in the `requires` or `skip` sections
that use cluster features; see [here](https://github.com/elastic/elasticsearch/blob/main/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/README.asciidoc#skipping-tests) for more information.

To aid with backwards compatibility tests, the test framework adds synthetic features
for each previously released Elasticsearch version, of the form `gte_v{VERSION}`
(for example `gte_v8.14.2`).
This can be used to add conditions based on previous releases. It _cannot_ be used
to check the current snapshot version; real features or capabilities should be
used instead.

## Capabilities

The Capabilities API is a REST API for external clients to check the capabilities
of an Elasticsearch cluster. As it is dynamically calculated for every query,
it is not limited in size or usage.

A capabilities query can be used to query for 3 things:
* Is this endpoint supported for this HTTP method?
* Are these parameters of this endpoint supported?
* Are these capabilities (arbitrary string ids) of this endpoint supported?

The API will return with a simple true/false, indicating if all specified aspects
of the endpoint are supported by all nodes in the cluster.
If any aspect is not supported by any one node, the API returns `false`.

The API can also return `supported: null` (indicating unknown)
if there was a problem communicating with one or more nodes in the cluster.

All registered endpoints automatically work with the endpoint existence check.
To add support for parameter and feature capability queries to your REST endpoint,
implement the `supportedQueryParameters` and `supportedCapabilities` methods in your rest handler.

To perform a capability query, perform a REST call to the `_capabilities` API,
with parameters `method`, `path`, `parameters`, `capabilities`.
The call will query every node in the cluster, and return `{supported: true}`
if all nodes support that specific combination of method, path, query parameters,
and endpoint capabilities. If any single aspect is not supported,
the query will return `{supported: false}`. If there are any problems
communicating with nodes in the cluster, the response will be `{supported: null}`
indicating support or lack thereof cannot currently be determined.
Capabilities can be checked using the clusterHasCapability method in ESRestTestCase.

Similar to cluster features, YAML tests can have skip and requires conditions
specified with capabilities like the following:

    - requires:
        capabilities:
          - method: GET
            path: /_endpoint
            parameters: [param1, param2]
            capabilities: [cap1, cap2]

method: GET is the default, and does not need to be explicitly specified.
