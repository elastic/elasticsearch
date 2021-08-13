/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.HasAttributeNodeSelector;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toCollection;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Represents a do section:
 *
 *   - do:
 *      catch:      missing
 *      headers:
 *          Authorization: Basic user:pass
 *          Content-Type: application/json
 *      warnings|warnings_regex:
 *          - Stuff is deprecated, yo
 *          - Don't use deprecated stuff
 *          - Please, stop. It hurts.
 *          - The non _regex version exact matches against the warning text and no need to worry about escaped quotes or backslashes
 *          - The _regex version matches against the raw value of the warning text which may include backlashes and quotes escaped
 *      allowed_warnings|allowed_warnings_regex:
 *          - Maybe this warning shows up
 *          - But it isn't actually required for the test to pass.
 *          - The non _regex version should be preferred for simplicity and performance.
 *      update:
 *          index:  test_1
 *          type:   test
 *          id:     1
 *          body:   { doc: { foo: bar } }
 *
 */
public class DoSection implements ExecutableSection {
    public static DoSection parse(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;

        DoSection doSection = new DoSection(parser.getTokenLocation());
        ApiCallSection apiCallSection = null;
        NodeSelector nodeSelector = NodeSelector.ANY;
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<String> expectedWarnings = new ArrayList<>();
        List<Pattern> expectedWarningsRegex = new ArrayList<>();
        List<String> allowedWarnings = new ArrayList<>();
        List<Pattern> allowedWarningsRegex = new ArrayList<>();

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected [" + XContentParser.Token.START_OBJECT + "], " +
                    "found [" + parser.currentToken() + "], the do section is not properly indented");
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("catch".equals(currentFieldName)) {
                    doSection.setCatch(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unsupported field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("warnings".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        expectedWarnings.add(parser.text());
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(), "[warnings] must be a string array but saw [" + token + "]");
                    }
                } else if ("warnings_regex".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        expectedWarningsRegex.add(Pattern.compile(parser.text()));
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[warnings_regex] must be a string array but saw [" + token + "]");
                    }
                } else if ("allowed_warnings".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        allowedWarnings.add(parser.text());
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[allowed_warnings] must be a string array but saw [" + token + "]");
                    }
                } else if ("allowed_warnings_regex".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        allowedWarningsRegex.add(Pattern.compile(parser.text()));
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "[allowed_warnings_regex] must be a string array but saw [" + token + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unknown array [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("headers".equals(currentFieldName)) {
                    String headerName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            headerName = parser.currentName();
                        } else if (token.isValue()) {
                            headers.put(headerName, parser.text());
                        }
                    }
                } else if ("node_selector".equals(currentFieldName)) {
                    String selectorName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            selectorName = parser.currentName();
                        } else {
                            NodeSelector newSelector = buildNodeSelector(selectorName, parser);
                            nodeSelector = nodeSelector == NodeSelector.ANY ?
                                newSelector : new ComposeNodeSelector(nodeSelector, newSelector);
                        }
                    }
                } else if (currentFieldName != null) { // must be part of API call then
                    apiCallSection = new ApiCallSection(currentFieldName);
                    String paramName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            paramName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("body".equals(paramName)) {
                                String body = parser.text();
                                XContentParser bodyParser = JsonXContent.jsonXContent
                                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body);
                                //multiple bodies are supported e.g. in case of bulk provided as a whole string
                                while(bodyParser.nextToken() != null) {
                                    apiCallSection.addBody(bodyParser.mapOrdered());
                                }
                            } else {
                                apiCallSection.addParam(paramName, parser.text());
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("body".equals(paramName)) {
                                apiCallSection.addBody(parser.mapOrdered());
                            }
                        }
                    }
                }
            }
        }
        try {
            if (apiCallSection == null) {
                throw new IllegalArgumentException("client call section is mandatory within a do section");
            }
            for (String w : expectedWarnings) {
                if (allowedWarnings.contains(w)) {
                    throw new IllegalArgumentException("the warning [" + w + "] was both allowed and expected");
                }
            }
            for (Pattern p : expectedWarningsRegex) {
                if (allowedWarningsRegex.contains(p)) {
                    throw new IllegalArgumentException("the warning pattern [" + p + "] was both allowed and expected");
                }
            }
            apiCallSection.addHeaders(headers);
            apiCallSection.setNodeSelector(nodeSelector);
            doSection.setApiCallSection(apiCallSection);
            doSection.setExpectedWarningHeaders(unmodifiableList(expectedWarnings));
            doSection.setExpectedWarningHeadersRegex(unmodifiableList(expectedWarningsRegex));
            doSection.setAllowedWarningHeaders(unmodifiableList(allowedWarnings));
            doSection.setAllowedWarningHeadersRegex(unmodifiableList(allowedWarningsRegex));
        } finally {
            parser.nextToken();
        }
        return doSection;
    }

    private static final Logger logger = LogManager.getLogger(DoSection.class);

    private final XContentLocation location;
    private String catchParam;
    private ApiCallSection apiCallSection;
    private List<String> expectedWarningHeaders = emptyList();
    private List<Pattern> expectedWarningHeadersRegex = emptyList();
    private List<String> allowedWarningHeaders = emptyList();
    private List<Pattern> allowedWarningHeadersRegex = emptyList();

    public DoSection(XContentLocation location) {
        this.location = location;
    }

    public String getCatch() {
        return catchParam;
    }

    public void setCatch(String catchParam) {
        this.catchParam = catchParam;
    }

    public ApiCallSection getApiCallSection() {
        return apiCallSection;
    }

    void setApiCallSection(ApiCallSection apiCallSection) {
        this.apiCallSection = apiCallSection;
    }

    /**
     * Warning headers patterns that we expect from this response.
     * If the headers don't match exactly this request is considered to have failed.
     * Defaults to emptyList.
     */
    List<String> getExpectedWarningHeaders() {
        return expectedWarningHeaders;
    }

    /**
     * Warning headers patterns that we expect from this response.
     * If the headers don't match this request is considered to have failed.
     * Defaults to emptyList.
     */
    List<Pattern> getExpectedWarningHeadersRegex() {
        return expectedWarningHeadersRegex;
    }

    /**
     * Set the warning headers that we expect from this response. If the headers don't match exactly this request is considered to have
     * failed. Defaults to emptyList.
     */
    void setExpectedWarningHeaders(List<String> expectedWarningHeaders) {
        this.expectedWarningHeaders = expectedWarningHeaders;
    }

    /**
     * Set the warning headers patterns that we expect from this response. If the headers don't match this request is considered to have
     * failed. Defaults to emptyList.
     */
    void setExpectedWarningHeadersRegex(List<Pattern> expectedWarningHeadersRegex) {
        this.expectedWarningHeadersRegex = expectedWarningHeadersRegex;
    }

    /**
     * Warning headers that we allow from this response. These warning
     * headers don't cause the test to fail. Defaults to emptyList.
     */
    List<String> getAllowedWarningHeaders() {
        return allowedWarningHeaders;
    }

    /**
     * Warning headers that we allow from this response. These warning
     * headers don't cause the test to fail. Defaults to emptyList.
     */
    List<Pattern> getAllowedWarningHeadersRegex() {
        return allowedWarningHeadersRegex;
    }

    /**
     * Set the warning headers that we expect from this response. These
     * warning headers don't cause the test to fail. Defaults to emptyList.
     */
    void setAllowedWarningHeaders(List<String> allowedWarningHeaders) {
        this.allowedWarningHeaders = allowedWarningHeaders;
    }

    /**
     * Set the warning headers pattern that we expect from this response. These
     * warning headers don't cause the test to fail. Defaults to emptyList.
     */
    void setAllowedWarningHeadersRegex(List<Pattern> allowedWarningHeadersRegex) {
        this.allowedWarningHeadersRegex = allowedWarningHeadersRegex;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {

        if ("param".equals(catchParam)) {
            //client should throw validation error before sending request
            //lets just return without doing anything as we don't have any client to test here
            logger.info("found [catch: param], no request sent");
            return;
        }

        try {
            ClientYamlTestResponse response = executionContext.callApi(apiCallSection.getApi(), apiCallSection.getParams(),
                    apiCallSection.getBodies(), apiCallSection.getHeaders(), apiCallSection.getNodeSelector());
            if (Strings.hasLength(catchParam)) {
                String catchStatusCode;
                if (catches.containsKey(catchParam)) {
                    catchStatusCode = catches.get(catchParam).v1();
                } else if (catchParam.startsWith("/") && catchParam.endsWith("/")) {
                    catchStatusCode = "4xx|5xx";
                } else {
                    throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
                }
                fail(formatStatusCodeMessage(response, catchStatusCode));
            }
            final String testPath = executionContext.getClientYamlTestCandidate() != null
                ? executionContext.getClientYamlTestCandidate().getTestPath()
                : null;
            checkWarningHeaders(response.getWarningHeaders(), testPath);
        } catch(ClientYamlTestResponseException e) {
            ClientYamlTestResponse restTestResponse = e.getRestTestResponse();
            if (Strings.hasLength(catchParam) == false) {
                fail(formatStatusCodeMessage(restTestResponse, "2xx"));
            } else if (catches.containsKey(catchParam)) {
                assertStatusCode(restTestResponse);
            } else if (catchParam.length() > 2 && catchParam.startsWith("/") && catchParam.endsWith("/")) {
                //the text of the error message matches regular expression
                assertThat(formatStatusCodeMessage(restTestResponse, "4xx|5xx"),
                        e.getResponseException().getResponse().getStatusLine().getStatusCode(), greaterThanOrEqualTo(400));
                Object error = executionContext.response("error");
                assertThat("error was expected in the response", error, notNullValue());
                //remove delimiters from regex
                String regex = catchParam.substring(1, catchParam.length() - 1);
                assertThat("the error message was expected to match the provided regex but didn't",
                        error.toString(), matches(regex));
            } else {
                throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
            }
        }
    }

    void checkWarningHeaders(final List<String> warningHeaders) {
        checkWarningHeaders(warningHeaders, null);
    }

    /**
     * Check that the response contains only the warning headers that we expect.
     */
    void checkWarningHeaders(final List<String> warningHeaders, String testPath) {
        final List<String> unexpected = new ArrayList<>();
        final List<String> unmatched = new ArrayList<>();
        final List<String> missing = new ArrayList<>();
        final List<String> missingRegex = new ArrayList<>();
        // LinkedHashSet so that missing expected warnings come back in a predictable order which is nice for testing
        final Set<String> allowed = allowedWarningHeaders.stream()
                .map(HeaderWarning::escapeAndEncode)
                .collect(toCollection(LinkedHashSet::new));
        final Set<Pattern> allowedRegex = new LinkedHashSet<>(allowedWarningHeadersRegex);
        final Set<String> expected = expectedWarningHeaders.stream()
                .map(HeaderWarning::escapeAndEncode)
                .collect(toCollection(LinkedHashSet::new));
        final Set<Pattern> expectedRegex = new LinkedHashSet<>(expectedWarningHeadersRegex);
        for (final String header : warningHeaders) {
            final Matcher matcher = HeaderWarning.WARNING_HEADER_PATTERN.matcher(header);
            final boolean matches = matcher.matches();
            if (matches) {
                final String message = HeaderWarning.extractWarningValueFromWarningHeader(header, true);
                if (allowed.contains(message)) {
                    continue;
                }

                if (expected.remove(message)) {
                    continue;
                }
                boolean matchedRegex = false;

                for(Pattern pattern : new HashSet<>(expectedRegex)){
                    if(pattern.matcher(message).matches()){
                        matchedRegex = true;
                        expectedRegex.remove(pattern);
                        break;
                    }
                }
                for(Pattern pattern : allowedRegex){
                    if(pattern.matcher(message).matches()){
                        matchedRegex = true;
                        break;
                    }
                }
                if (matchedRegex){
                    continue;
                }
                unexpected.add(header);
            } else {
                unmatched.add(header);
            }
        }
        if (expected.isEmpty() == false) {
            for (final String header : expected) {
                missing.add(header);
            }
        }
        if (expectedRegex.isEmpty() == false) {
            for (final Pattern headerPattern : expectedRegex) {
                missingRegex.add(headerPattern.pattern());
            }
        }

        // Log and remove all deprecation warnings for legacy index templates as a shortcut to dealing with all the legacy
        // templates used in YAML tests. Once they have all been migrated to composable templates, this should be removed.
        for (Iterator<String> warnings = unexpected.iterator(); warnings.hasNext();) {
            if (warnings.next().endsWith(RestPutIndexTemplateAction.DEPRECATION_WARNING + "\"")) {
                logger.warn(
                    "Test [{}] uses deprecated legacy index templates and should be updated to use composable templates",
                    (testPath == null ? "<unknown>" : testPath) + ":" + getLocation().lineNumber
                );
                warnings.remove();
            }
        }

        if (unexpected.isEmpty() == false
            || unmatched.isEmpty() == false
            || missing.isEmpty() == false
            || missingRegex.isEmpty() == false) {
            final StringBuilder failureMessage = new StringBuilder();
            appendBadHeaders(failureMessage, unexpected, "got unexpected warning header" + (unexpected.size() > 1 ? "s" : ""));
            appendBadHeaders(failureMessage, unmatched, "got unmatched warning header" + (unmatched.size() > 1 ? "s" : ""));
            appendBadHeaders(failureMessage, missing, "did not get expected warning header" + (missing.size() > 1 ? "s" : ""));
            appendBadHeaders(failureMessage, missingRegex, "the following regular expression" + (missingRegex.size() > 1 ? "s" : "")
                + " did not match any warning header");
            fail(failureMessage.toString());
        }
    }

    private void appendBadHeaders(final StringBuilder sb, final List<String> headers, final String message) {
        if (headers.isEmpty() == false) {
            sb.append(message).append(" [\n");
            for (final String header : headers) {
                sb.append("\t").append(header).append("\n");
            }
            sb.append("]\n");
        }
    }

    private void assertStatusCode(ClientYamlTestResponse restTestResponse) {
        Tuple<String, org.hamcrest.Matcher<Integer>> stringMatcherTuple = catches.get(catchParam);
        assertThat(formatStatusCodeMessage(restTestResponse, stringMatcherTuple.v1()),
                restTestResponse.getStatusCode(), stringMatcherTuple.v2());
    }

    private String formatStatusCodeMessage(ClientYamlTestResponse restTestResponse, String expected) {
        String api = apiCallSection.getApi();
        if ("raw".equals(api)) {
            api += "[method=" + apiCallSection.getParams().get("method") + " path=" + apiCallSection.getParams().get("path") + "]";
        }
        return "expected [" + expected + "] status code but api [" + api + "] returned [" + restTestResponse.getStatusCode() +
                " " + restTestResponse.getReasonPhrase() + "] [" + restTestResponse.getBodyAsString() + "]";
    }

    private static Map<String, Tuple<String, org.hamcrest.Matcher<Integer>>> catches = new HashMap<>();

    static {
        catches.put("bad_request", tuple("400", equalTo(400)));
        catches.put("unauthorized", tuple("401", equalTo(401)));
        catches.put("forbidden", tuple("403", equalTo(403)));
        catches.put("missing", tuple("404", equalTo(404)));
        catches.put("request_timeout", tuple("408", equalTo(408)));
        catches.put("conflict", tuple("409", equalTo(409)));
        catches.put("unavailable", tuple("503", equalTo(503)));
        catches.put("request", tuple("4xx|5xx", allOf(greaterThanOrEqualTo(400),
                not(equalTo(400)),
                not(equalTo(401)),
                not(equalTo(403)),
                not(equalTo(404)),
                not(equalTo(408)),
                not(equalTo(409)))));
    }

    private static NodeSelector buildNodeSelector(String name, XContentParser parser) throws IOException {
        switch (name) {
        case "attribute":
            return parseAttributeValuesSelector(parser);
        case "version":
            return parseVersionSelector(parser);
        default:
            throw new XContentParseException(parser.getTokenLocation(), "unknown node_selector [" + name + "]");
        }
    }

    private static NodeSelector parseAttributeValuesSelector(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(parser.getTokenLocation(), "expected START_OBJECT");
        }
        String key = null;
        XContentParser.Token token;
        NodeSelector result = NodeSelector.ANY;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                key = parser.currentName();
            } else if (token.isValue()) {
                /*
                 * HasAttributeNodeSelector selects nodes that do not have
                 * attribute metadata set so it can be used against nodes that
                 * have not yet been sniffed. In these tests we expect the node
                 * metadata to be explicitly sniffed if we need it and we'd
                 * like to hard fail if it is not so we wrap the selector so we
                 * can assert that the data is sniffed.
                 */
                NodeSelector delegate = new HasAttributeNodeSelector(key, parser.text());
                NodeSelector newSelector = new NodeSelector() {
                    @Override
                    public void select(Iterable<Node> nodes) {
                        for (Node node : nodes) {
                            if (node.getAttributes() == null) {
                                throw new IllegalStateException("expected [attributes] metadata to be set but got "
                                        + node);
                            }
                        }
                        delegate.select(nodes);
                    }

                    @Override
                    public String toString() {
                        return delegate.toString();
                    }
                };
                result = result == NodeSelector.ANY ?
                    newSelector : new ComposeNodeSelector(result, newSelector);
            } else {
                throw new XContentParseException(parser.getTokenLocation(), "expected [" + key + "] to be a value");
            }
        }
        return result;
    }

    private static NodeSelector parseVersionSelector(XContentParser parser) throws IOException {
        if (false == parser.currentToken().isValue()) {
            throw new XContentParseException(parser.getTokenLocation(), "expected [version] to be a value");
        }
        List<VersionRange> skipVersionRanges = parser.text().equals("current")
            ? List.of(new VersionRange(Version.CURRENT, Version.CURRENT))
            : SkipSection.parseVersionRanges(parser.text());
        return new NodeSelector() {
            @Override
            public void select(Iterable<Node> nodes) {
                for (Iterator<Node> itr = nodes.iterator(); itr.hasNext();) {
                    Node node = itr.next();
                    if (node.getVersion() == null) {
                        throw new IllegalStateException("expected [version] metadata to be set but got "
                                + node);
                    }
                    Version version = Version.fromString(node.getVersion());
                    boolean skip = skipVersionRanges.stream().anyMatch(v -> v.contains(version));
                    if (false == skip) {
                        itr.remove();
                    }
                }
            }

            @Override
            public String toString() {
                return "version ranges "+skipVersionRanges;
            }
        };
    }

    /**
     * Selector that composes two selectors, running the "right" most selector
     * first and then running the "left" selector on the results of the "right"
     * selector.
     */
    private static class ComposeNodeSelector implements NodeSelector {
        private final NodeSelector lhs;
        private final NodeSelector rhs;

        private ComposeNodeSelector(NodeSelector lhs, NodeSelector rhs) {
            this.lhs = Objects.requireNonNull(lhs, "lhs is required");
            this.rhs = Objects.requireNonNull(rhs, "rhs is required");
        }

        @Override
        public void select(Iterable<Node> nodes) {
            rhs.select(nodes);
            lhs.select(nodes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComposeNodeSelector that = (ComposeNodeSelector) o;
            return Objects.equals(lhs, that.lhs) &&
                    Objects.equals(rhs, that.rhs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lhs, rhs);
        }

        @Override
        public String toString() {
            // . as in haskell's "compose" operator
            return lhs + "." + rhs;
        }
    }
}
