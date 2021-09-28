#  REST API compatibility developers guide

REST API compatibility is intended to minimize the impact for breaking changes applied to the REST API. Compatibility is implemented in a best effort to strike a balance between preserving the REST API contract across major versions while still allowing for breaking changes. URL paths, parameters, HTTP verbs, request bodies and response bodies are all covered by REST API compatibility.

### Example use case

The recommended procedure to upgrade Elasticsearch to a new major version is to first upgrade Elasticsearch (the server), then upgrade any of the consumers/clients. This implies that the consumers/clients during and immediately after an upgrade may still be attempting to communicate with Elasticsearch using the prior major version's REST API contract.  During this time if any of consumers/clients continue to call into one of the REST API's that include breaking changes, the consumer/client can also break.  Compatibility is a best attempt to intended honor the spirit of the original API call even after the breaking change has been applied.

For example, assume a REST request requires a consumer to send a "limit" parameter in the body of the request. Elasticsearch (the server) is upgraded to the next major version and that major version made a non-passive change that removes "limit" in favor of a "minimum" and "maximum" fields to enable some new functionality to also support a new lower limit. After the major version upgrade Elasticsearch only knows about "minimum" and "maximum", but the consumer/client (still on the older version of the client) only knows about "limit". With REST API compatibility the consumer/client can continue to send "limit" to the same API and Elasticsearch (the server) will honor that request using the value of "limit" as the "maximum".  Since the prior version had no notion of "minimum",  mapping "limit" -> "maximum" is the best attempt to honor the spirit of the request while still allowing the API and underlying behavior to evolve.  A warning will emitted to let the consumer/client know Elasticsearch applied compatibility.

### Workflow

REST API compatibility is opt-in per request using a specialized value for the `Accept` or `Content-Type` HTTP header.  The intent is that this header may be sent prior to a major version upgrade resulting in no differences with the standard header values when the client and server match versions. For example, assume the client is speaking in terms of the v7 REST API and Elasticsearch (the server) is running v7 then sending the specialized header value will have no effect. However, once Elasticsearch (the server) is upgraded to v8, compatibility will kick in and with best effort honor the v7 REST API contract. This allows Elasticsearch (the server) to be upgraded prior to the client/consumer upgrading, even they are still using deprecated functionality.

Breaking changes always follow a lifecycle of deprecation (documentation and warnings) in the current version and in most cases will also expose the new functionality that causes the deprecation also in the current version.  The next major version will apply the breaking change. This give users an opportunity to adopt the non-deprecated variants in the current version. It is the still recommended approach to stop the usage of all deprecated functionality prior to a major version upgrade. However, in practice this can be a difficult, error prone, and a time consuming task.  So it also recommended that consumers/clients send the specialized header to enable REST API compatibility to help catch any missed usages of deprecated functionality.

### Specialized header

REST API compatibility is enabled per request using a specialized HTTP header.

For a request without a body the following Accept header is required to request REST API compatibility  (examples for compatibility with the v7 REST API contract ) :

```javascript
Accept: "application/vnd.elasticsearch+json;compatible-with=7"
```

If the request also has a body the following is also necessary

```javascript
Content-Type: "application/vnd.elasticsearch+json;compatible-with=7"
```

The headers mirrors the 4 supported media types for both the `Accept` and `Content-Type` headers.  (examples for compatibility with the v7 REST API contract).

```javascript
"application/vnd.elasticsearch+json;compatible-with=7"
"application/vnd.elasticsearch+yaml;compatible-with=7"
"application/vnd.elasticsearch+smile;compatible-with=7"
"application/vnd.elasticsearch+cbor;compatible-with=7"
```

A consumer/client may not mix "compatible-with" versions between headers. Attempts to mix and match compatible versions between `Accept` and `Content-Type` headers will result in an error.  A consumer/client may mix the media type (i.e. send `yaml` and get back `json`).



## Introducing breaking changes to the REST API

The rest of this guide is intended to help Elasticsearch developers understand how to introduce breaking changes to the REST API with support for REST API compatibility.

### When to apply

Any changes that touch the URL path,  URL parameters ,HTTP verbs, the shape or response code for the non-error response, the shape of the request should account for REST API compatibility.  REST API compatibility is first introduced with v8 with compatibility back to v7.  The v7 branch of code has some minimal support to allow for easy back porting and to allow for the reading of the header on a v7 cluster. There are no plans to provide compatibility back to v6.

REST API compatibility is not the same as a fully version-ed API. It is a best attempt find a compatible way to honor the prior major version REST API contract. There will be cases where it is not possible to apply a compatibility. In those cases a meaningful error message should be emitted.

### When not to apply

Settings, SQL, scripting, and errors messages all touch the REST API but are not covered by REST API compatibility.

### Implementation

There are 4 primary integration points with REST API compatibility.

*  Serializing responses to xContent
*  De-serializing requests from xContent
*  URL path, URL parameters, and HTTP verbs
*  Testing for compatibility

### Serializing

Emitting a response back to the client is generally done via the `ToXContent#toXContent` method. The requested compatibility can be found in the `XContentBuilder` object allowing for conditional logic based on a specific request. For example:

```java
    if (builder.getRestApiVersion() == RestApiVersion.V_7) {
        builder.field("limit", max);
    } else {
        builder.field("maximum,", max);
        builder.field("minimum", min);
    }
```

In some cases the `ToXContent#toXContent` is also used to serialize the values to the cluster state.  In these cases some refactoring or additional runtime validation is necessary to prevent the REST API conditional serialization from persisting in cluster state.

In places where `ToXContent#toXContent` is not used for serialization, then the requested compatibility can also be found in the `RestRequest` object and used accordingly. It is generally discouraged to push the requested compatibility version down through the transport actions to allow keeping the REST compatibility a REST layer concern.

### De-serializing

Accepting a payload from a prior version's REST request is a bit more difficult sending a compatible response. There are a couple different approaches for de-serialization, however, in general all of them use a request specific parser.  `XContentParser` has a `getRestApiVersion()` method that could be used in manner that is near identical to serialization. However, in practice most parsers are invoked via a `ConstructingObjectParser` or an `ObjectParser` and quite often they are daisy chained together via the `NamedXContentRegistry`.

Both `ConstructingObjectParser` and `ObjectParser` use `ParseField`'s to statically declare the relationship between a named key and value pairing. A `ParseField` can be made version aware, meaning that it will match on the incoming payload if the requested compatibility is the correct version.

For example:

```java
PARSER.declareInt(MyPojo::setMax,
       new ParseField("maximum", "limit").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7)));
PARSER.declareInt(MyPojo::setMax,
       new ParseField("maximum").forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.V_8)));
```

The above example is for code that live in the v8 branch of code. It reads the `maximum` value from the request for both v7 and v8. However, if compatibility is requested it will also allow `limit` in the payload.  If `limit` is used a warning will be emitted.

The version in `forRestApiVersion` is reference to when the declaration is valid. Assuming v8 is the master branch and all changes start in the master branch then get back ported. The above text is what would be applicable for the v8 branch of code. The first line of code is essentially ignored except for when compatibility with v7 is requested. When back-porting this change to the 7.x branch, the first line would be identical, and the second line would be omitted.

The above strategy works well for single fields, but could get overly complex very fast for large multiple field changes. For more complex de-serialization changes there is also support to construct a `NamedXContentRegistry` with some "normal" entries as well some entries that are only applied when compatibility with the prior version is requested. Additionally there is a pseudo standard method `fromXContent` which also has access to the `XContentParser` for that request which can be used to conditionally change how to parse the input.

### URL

Paths are declared via a Route. Routes are composed of the HTTP verb and the relative path. Optionally they can declare a deprecated verb/path combination and the REST API version in which they were deprecated.

For example:

```java
   Route.builder(GET, "_mypath/{foo}/{bar}").deprecated(MY_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
```

The above declares that `GET _mypath/foo/bar` is deprecated in v7. When this path is called in a v7 server, it will emit the deprecation warning. When this path is called in a v8 server, it will throw a 404 error unless REST API compatibility is requested. Only then will the path be honored and will also emit a warning message.



HTTP parameters (i.e. ?user=alice) are also covered by REST API compatibility and must be at least consumed as to not cause an error. For example if you remove a parameter in v8, you must at least read that parameter when v7 compatibility is requested.

For example:

```java
if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("limit")) {
    deprecationLogger.compatibleApiWarning("limit_parameter_deprecation",
                      "Deprecated parameter [limit] used, replaced by [maximum and minimum]");
    setMax(request.param("limit"));
 }
```

The above only applies when compatibility is requested and the request has the parameter.  Here the deprecation warning is not automatic and requires the developer to manually log the warning. `request.param` consumes the value as to avoid the error of unconsumed parameters.

### Testing

The primary means of testing compatibility is via the YAML REST tests. The build system will download the latest prior version of the YAML rest tests and execute them against the current cluster version. Prior to execution the tests will be transformed by injecting the correct headers to enable compatibility as well as other custom changes to the tests to allow the tests to pass or to test additional attributes. These customizations are configured via the build.gradle and happen just prior to test execution. Since there is some manipulation of the tests prior to execution, it is important to find the local (on disk) version for troubleshooting (instead of just the source tests from Github).

The tests are wired into the `check` task, so that is the easiest way to test locally prior to committing.  More specifically the task is called `yamlRestCompatTest` and behaves similarly to it's non-compat `yamlRestTest` task. Since these are a variation of backward compatibility testing, the tests will be skipped anytime the backward compatibility testing is disabled. Since the source code for these tests live in a branch of code, disabling a specific test should be done via the REST test blacklist found in the build.gradle. (as opposed to the standard skip section of the tests).

If running from inside IntelliJ, find the `ESClientYamlSuiteTestCase` in the `yamlRestTest` folder, right click to run with gradle, and then pick the `Compat` variant from the pop-up box.

In some cases the prior version of the YAML REST tests are not sufficient to fully test changes. This can happen when the prior version has insufficient test coverage. In those cases, you can simply add more testing to the prior version or you can add custom REST tests that will run along side of the other compatibility tests (just place them in the correct path in the `resources/rest-api-spec/test/v7compat`). Note - custom REST tests for compatibility will not be modified prior to execution, so the correct headers need to be manually added.

### Developer's workflow

There should not be much, if any deviation in a developers normal workflow to introduce and back-port changes. Changes should be applied in master, then back ported as needed.

Most of the compatibility will work correctly when back-porting as-is, but some care is needed that the logic is correct for that version when back-porting.  For example, both the route (URL) and field (de-serialization) declarations with version awareness will behave differently if the declared version is the current version or the prior version. This allows the same line of code to be back ported as-is with differing behavior.  Additionally the compatible version is always populated (even when not requested, defaulting to the current version), so conditional logic comparing against a specific version is safe across branches.

Mixed clusters are not explicitly tested since the change should be applied at the REST (coordinate node) layer.

### Troubleshooting compatibility test failures

By far the most common reason that compatibility tests can seemingly randomly fail is that your master branch is out of date with the upstream master.  For example, developer A introduced a breaking change and as part of that deprecated some API in N-1 version. As part of that change, the developer changed the N-1 test to expect the deprecation warning.  When you run the compatibility tests from your out-of-date master branch it will pull the most recent version of N-1 and try to execute against a test cluster that is out of date. The out of date test cluster does not include the deprecated/removed end point so the test fails.  Update your branch with the latest master should resolve that specific issue.

Test failure reproduction lines should behave identical to the non-compatible variant. However, to assure you are referencing the correct line number when reading the test, be sure to look at the line number from test on disk.  Generally the fully transformed tests can be found at `build/restResources/v7/yamlTests/transformed/rest-api-spec/test/*` (where v7 will change with different versions).

Muting compatibility tests need to be done via a blacklist in the correct `build.gradle`. For example:

```groovy
tasks.named("yamlRestCompatTest").configure {
  systemProperty 'tests.rest.blacklist', [
    'repository_url/10_basic/Get a non existing snapshot',
    'repository_url/10_basic/Restore with repository-url using http://',
    'repository_url/10_basic/Restore with repository-url using file://'
  ].join(',')
}
```













