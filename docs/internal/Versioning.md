Versioning Elasticsearch
========================

Elasticsearch is a complicated product, and is run in many different scenarios.
A single version number is not sufficient to cover the whole of the product,
instead we need different concepts to provide versioning capabilities
for different aspects of Elasticsearch, depending on their scope, updatability,
responsiveness, and maintenance.

## Release version

This is the version number used for published releases of Elasticsearch,
and the Elastic stack. This takes the form _major.minor.patch_,
with a corresponding version id.

Uses of this version number should be avoided, as it does not apply to
some scenarios, and use of release version will break Elasticsearch nodes.

The release version is accessible in code through `Build.current().version()`,
but it **should not** be assumed that this is a semantic version number,
it could be any arbitrary string.

## Transport protocol

The transport protocol is used to send binary data between Elasticsearch nodes; a
`TransportVersion` encapsulates versioning of this protocol.
This version is negotiated between each pair of nodes in the cluster
on first connection, selecting the highest shared version.
This version is then accessible through the `getTransportVersion` method
on `StreamInput` and `StreamOutput`, so serialization code can read/write
objects in a form that will be understood by the other node.

At a high level a `TransportVersion` contains one id per release branch it will
be committed to. Each `TransportVersion` has a name selected when it is generated.
In order to ensure consistency and robustness, all new `TransportVersion`s
must first be created in the `main` branch and then backported to the relevant
release branches.

_Elastic developers_ - instructions in this section for managing transport
versions are the same in Serverless.

### Internal state files

The Elasticsearch server jar contains resource files representing each
transport version. These files are loaded at runtime to construct
`TransportVersion` instances. Since each transport version has its own file
they can be backported without conflict.

Additional resource files represent the latest transport version known on
each release branch. If two transport versions are added at the same time,
there will be a conflict in these internal state files, forcing one to be
regenerated to resolve the conflict before merging to `main`.

All of these internal state files are managed by gradle tasks; they should
not be edited directly.

### Creating transport versions locally

To create a transport version, declare a reference anywhere in java code. For example:

    private static final TransportVersion MY_NEW_TV = TransportVersion.fromName("my_new_tv");

`fromName` takes an arbitrary String name. The String must be a String literal;
it cannot be a reference to a String. It must match the regex `[_0-9a-zA-Z]+`.
You can reference the same transport version name from multiple classes, but
you must not use an existing transport version name after it as already been
committed to `main`.

Once you have declared your `TransportVersion` you can use it in serialization code.
For example, in a constructor that takes `StreamInput in`:

    if (in.getTransportVersion().supports(MY_NEW_TV)) {
        // new serialization code
    }

Finally, in order to run Elasticsearch or run tests, the transport version ids
must be generated. Run the following gradle task:

    ./gradlew generateTransportVersion

This will generate the internal state to support the new transport version.
A new file will be created for the transport version:

    server/src/main/resources/transport/definitions/referable/my_new_tv.csv

Additionally, the upper bounds for the current branch's minor version will be
updated to include the new transport version. For example, if main is `9.4` then
the following upper bound file will have modifications:

    server/src/main/resources/transport/upper_bounds/9.4.csv

If you also intend to backport your code, include branches you will backport to:

    ./gradlew generateTransportVersion --backport-branches=9.1,8.19

This will update the upper bounds for those branches as well. You will see
modifications to the following files:

    server/src/main/resources/transport/upper_bounds/9.1.csv
    server/src/main/resources/transport/upper_bounds/8.19.csv

### Updating transport versions

You can modify a transport version before it is merged to `main`. This includes
renaming the transport version, updating the branches it will be backported to,
or even removing the transport version itself.

The generation task is idempotent. It can be re-run at any time and will result
in a valid internal state. For example, if you want to add an additional
backport branch, re-run the generation task with all the target backport
branches:

    ./gradlew generateTransportVersion --backport-branches=9.1,9.0,8.19,8.18

You can also let CI handle updating transport versions for you. As version
labels are updated on your PR, the generation task is automatically run with
the appropriate backport branches and any changes to the internal state files
are committed to your branch.

Transport versions can also have additional branches added after merging to
`main`. When doing so, you must include all branches the transport version was
added to in addition to new branch. For example, if you originally committed
your transport version `my_tv` to `main` and `9.1`, and then realized you also
needed to backport to `8.19` you would run (in `main`):

    ./gradlew generateTransportVersion --name=my_tv --backport-branches=9.1,8.19

This will update the existing definition file and the upper bounds for the newly
added branch. You will see modifications to the following files:

    server/src/main/resources/transport/definitions/referable/my_tv.csv
    server/src/main/resources/transport/upper_bounds/8.19.csv

In the above case CI will not know what transport version name to update, so you
must run the generate task again as described. After merging the updated
transport version it will need to be backported to all the applicable branches.
Backport branches that already included the original change can use
auto-backport in CI or cherry-pick the updated transport version. Backport
branches that did not include the original change need to cherry-pick both the
original change and the updated transport version. For example, with the above
change that is first backported to `9.1`, and subsequently added to `8.19`, the
sequence of events would look like the following:

1. Create a pull request with target branch `main` for a new change
(`C0`) with a newly created transport version (`TV`) required by `C0`
   1. Use the `github` labels `auto-backport` and `branch:9.1` to backport `C0`
   and `TV` into the target branch `9.1`.
2. Discover that `C0` is also required in target branch `8.19`.
3. Create a pull request with target branch `main` for a change (`C1`) to only
update `TV` to include `8.19`
   1. Use the gradle task
   `./gradlew generateTransportVersion --name=my_tv
   --backport-branches=9.1,8.19`
   to update `TV` to include `8.19`
   2. Use the `github` labels `auto-backport`, `branch:9.1`, and `branch:8.19`
   to backport `C1` and updated `TV` into the target branches `9.1` and `8.19`.
   This creates a source branch for `8.19` (`SB819`) that does not contain `C0`.
   3. Cherry-pick `C0` into the source branch `SB819`

Note there are several ways to achieve the desired end state. This
specific example takes advantage of the available CI tooling to simplify the
process.

### Resolving merge conflicts

Transport versions are created sequentially. If two developers create a transport
version at the same time, based on the same `main` commit, they will generate
the same internal ids. The first of these two merged into `main` will "win", and
the latter will have a merge conflict with `main`.

In the event of a conflict, merge `main` into your branch. You will have
conflict(s) with transport version internal state files. Run the following
task to resolve the conflict(s):

    ./gradlew resolveTransportVersionConflict

This command will regenerate your transport version and stage the updated
state files in git. For a transport version named `my_new_tv` and the main
branch using version `9.4` the following files will be updated:

    server/src/main/resources/transport/definitions/referable/my_new_tv.csv
    server/src/main/resources/transport/upper_bounds/9.4.csv

You can then proceed with your merge as usual.

### Backport conflicts

When backporting a change using `git cherry-pick`, you may encounter conflicts in
the transport version internal state files. This happens when the main branch
has had other transport versions added since the last transport version was
backported to the release branch.

In this case, use the same task as for merge conflicts:

    ./gradlew resolveTransportVersionConflict

This command will apply the changes from the original PR to the release branch
being backported to, fixing the transport version state files as necessary for
that branch, and staging those files.

### Reverting changes

Transport versions cannot be removed, they can only be added. If the logic
using a transport version needs to be reverted, it must be done with a
new transport version.

For example, if you have previously added a transport version named
`original_tv` you could add `revert_tv` reversing the logic:

    TransportVersion tv = in.getTransportVersion();
    if (tv.supports(ORIGINAL_TV) && tv.supports(REVERT_TV) == false) {
        // serialization code being reverted
    }

### Minimum compatibility versions

The transport version used between two nodes is determined by the initial handshake
(see `TransportHandshaker`, where the two nodes swap their highest known transport version).
The lowest transport version that is compatible with the current node
is determined by `TransportVersion.minimumCompatible()`,
and the node is prevented from joining the cluster if it is below that version.
This constant should be updated manually on a major release.

The minimum version that can be used for CCS is determined by
`TransportVersion.minimumCCSVersion()`, but this is not actively checked
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
metadata, and associated mappings. It is declared with the pattern `M_NNN_S_PP`,
for the major version, version id, subsidiary version id, and patch number
respectively.

Index version is stored in index metadata when an index is created,
and it is used to determine the storage format and what functionality that index supports.
The index version does not change once an index is created.

When a change is needed to the index data format or metadata, or new mapping
types are added, create a new version constant below the last one, incrementing
the `NNN` version component.

Index version constants cannot be collapsed together,
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
