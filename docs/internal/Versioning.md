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

The transport protocol is used to send binary data between Elasticsearch nodes;
`TransportVersion` is the version number used for this protocol.
This version number is negotiated between each pair of nodes in the cluster
on first connection, and is set as the lower of the highest transport version
understood by each node.
This version is then accessible through the `getTransportVersion` method
on `StreamInput` and `StreamOutput`, so serialization code can read/write
objects in a form that will be understood by the other node.

Every change to the transport protocol is represented by a new transport version,
higher than all previous transport versions, which then becomes the highest version
recognized by that build of Elasticsearch. The version ids are stored
as constants in the `TransportVersions` class.
Each id has a standard pattern `M_NNN_S_PP`, where:
* `M` is the major version
* `NNN` is an incrementing id
* `S` is used in subsidiary repos amending the default transport protocol
* `PP` is used for patches and backports

When you make a change to the serialization form of any object,
you need to create a new sequential constant in `TransportVersions`,
introduced in the same PR that adds the change, that increments
the `NNN` component from the previous highest version,
with other components  set to zero.
For example, if the previous version number is `8_413_0_01`,
the next version number should be `8_414_0_00`.

Once you have defined your constant, you then need to use it
in serialization code. If the transport version is at or above the new id,
the modified protocol should be used:

    str = in.readString();
    bool = in.readBoolean();
    if (in.getTransportVersion().onOrAfter(TransportVersions.NEW_CONSTANT)) {
        num = in.readVInt();
    }

If a transport version change needs to be reverted, a **new** version constant
should be added representing the revert, and the version id checks
adjusted appropriately to only use the modified protocol between the version id
the change was added, and the new version id used for the revert (exclusive).
The `between` method can be used for this.

Once a transport change with a new version has been merged into main or a release branch,
it **must not** be modified - this is so the meaning of that specific
transport version does not change.

_Elastic developers_ - please see corresponding documentation for Serverless
on creating transport versions for Serverless changes.

### Collapsing transport versions

As each change adds a new constant, the list of constants in `TransportVersions`
will keep growing. However, once there has been an official release of Elasticsearch,
that includes that change, that specific transport version is no longer needed,
apart from constants that happen to be used for release builds.
As part of managing transport versions, consecutive transport versions can be
periodically collapsed together into those that are only used for release builds.
This task is normally performed by Core/Infra on a semi-regular basis,
usually after each new minor release, to collapse the transport versions
for the previous minor release. An example of such an operation can be found
[here](https://github.com/elastic/elasticsearch/pull/104937).

#### Tips

- We collapse versions only on the `main` branch.
- Use [TransportVersions.csv](../../server/src/main/resources/org/elasticsearch/TransportVersions.csv) as your guide.
- For each release version listed in that file with a corresponding `INITIAL_ELASTICSEARCH_` entry corresponding to that version,
  - change the prefix to `V_`
  - replace all intervening entries since the previous patch `V_` entry with the new `V_` entry
  - look at all uses of the new `V_` entry and look for dead code that can be deleted

For example, if there's a version `1.2.3` in the CSV file,
and `TransportVersions.java` has an entry called `INITIAL_ELASTICSEARCH_1_2_3`,
then:
- rename it to `V_1_2_3`,
- replace any other intervening symbols between `V_1_2_2` and `V_1_2_3` with `V_1_2_3` itself, and
- look through all the uses of the new `V_1_2_3` symbol; if any contain dead code, simplify it.

When in doubt, you can always leave the code as-is.
This is an optional cleanup step that is never required for correctness.

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

### Managing patches and backports

Backporting transport version changes to previous releases
should only be done if absolutely necessary, as it is very easy to get wrong
and break the release in a way that is very hard to recover from.

If we consider the version number as an incrementing line, what we are doing is
grafting a change that takes effect at a certain point in the line,
to additionally take effect in a fixed window earlier in the line.

To take an example, using indicative version numbers, when the latest
transport version is 52, we decide we need to backport a change done in
transport version 50 to transport version 45. We use the `P` version id component
to create version 45.1 with the backported change.
This change will apply for version ids 45.1 to 45.9 (should they exist in the future).

The serialization code in the backport needs to use the backported protocol
for all version numbers 45.1 to 45.9. The `TransportVersion.isPatchFrom` method
can be used to easily determine if this is the case: `streamVersion.isPatchFrom(45.1)`.
However, the `onOrAfter` also does what is needed on patch branches.

The serialization code in version 53 then needs to additionally check
version numbers 45.1-45.9 to use the backported protocol, also using the `isPatchFrom` method.

As an example, [this transport change](https://github.com/elastic/elasticsearch/pull/107862)
was backported from 8.15 to [8.14.0](https://github.com/elastic/elasticsearch/pull/108251)
and [8.13.4](https://github.com/elastic/elasticsearch/pull/108250) at the same time
(8.14 was a build candidate at the time).

The 8.13 PR has:

    if (transportVersion.onOrAfter(8.13_backport_id))

The 8.14 PR has:

    if (transportVersion.isPatchFrom(8.13_backport_id)
        || transportVersion.onOrAfter(8.14_backport_id))

The 8.15 PR has:

    if (transportVersion.isPatchFrom(8.13_backport_id)
        || transportVersion.isPatchFrom(8.14_backport_id)
        || transportVersion.onOrAfter(8.15_transport_id))

In particular, if you are backporting a change to a patch release,
you also need to make sure that any subsequent released version on any branch
also has that change, and knows about the patch backport ids and what they mean.

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
