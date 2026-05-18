/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.TimeUnits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlDocsTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.is;

//The default 20 minutes timeout isn't always enough, but Darwin CI hosts are incredibly slow...
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class DocsClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    /**
     * Resolve a path against the docs project directory. The {@code tests.docs.dir} system
     * property is wired up in {@code docs/build.gradle} and points to the docs project's
     * {@code projectDir}, so we can keep referencing the existing files in {@code src/test/cluster/config}
     * and friends without copying them onto the test classpath.
     */
    private static Path docsFile(String relative) {
        return Path.of(System.getProperty("tests.docs.dir"), relative);
    }

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .keystorePassword("keystore-password")
        // debug ccr test failures:
        // https://github.com/elastic/elasticsearch/issues/95678
        // https://github.com/elastic/elasticsearch/issues/94359
        // https://github.com/elastic/elasticsearch/issues/96561
        .setting("logger.org.elasticsearch.transport.SniffConnectionStrategy", "DEBUG")
        .setting("logger.org.elasticsearch.transport.RemoteClusterService", "DEBUG")
        // enable regexes in painless so our tests don't complain about example snippets that use them
        .setting("script.painless.regex.enabled", "true")
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        // Whitelist reindexing from the local node so we can test it.
        .setting("reindex.remote.whitelist", "127.0.0.1:*")
        // TODO: remove this once cname is prepended to transport.publish_address by default in 8.0
        .systemProperty("es.transport.cname_in_publish_address", "true")
        .systemProperty("es.queryable_built_in_roles_enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // disable the ILM and SLM history for doc tests to avoid potential lingering tasks that'd cause test flakiness
        .setting("indices.lifecycle.history_index_enabled", "false")
        .setting("slm.history_index_enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.realms.file.file.order", "0")
        .setting("xpack.security.authc.realms.native.native.order", "1")
        .setting("xpack.security.authc.realms.oidc.oidc1.order", "2")
        .setting("xpack.security.authc.realms.oidc.oidc1.op.issuer", "http://127.0.0.1:8080")
        .setting("xpack.security.authc.realms.oidc.oidc1.op.authorization_endpoint", "http://127.0.0.1:8080/c2id-login")
        .setting("xpack.security.authc.realms.oidc.oidc1.op.token_endpoint", "http://127.0.0.1:8080/c2id/token")
        .setting("xpack.security.authc.realms.oidc.oidc1.op.jwkset_path", "op-jwks.json")
        .setting("xpack.security.authc.realms.oidc.oidc1.rp.redirect_uri", "https://my.fantastic.rp/cb")
        .setting("xpack.security.authc.realms.oidc.oidc1.rp.client_id", "elasticsearch-rp")
        .keystore(
            "xpack.security.authc.realms.oidc.oidc1.rp.client_secret",
            "b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2"
        )
        .setting("xpack.security.authc.realms.oidc.oidc1.rp.response_type", "id_token")
        .setting("xpack.security.authc.realms.oidc.oidc1.claims.principal", "sub")
        .setting("xpack.security.authc.realms.pki.pki1.order", "3")
        .setting("xpack.security.authc.realms.pki.pki1.certificate_authorities", "[ \"testClient.crt\" ]")
        .setting("xpack.security.authc.realms.pki.pki1.delegation.enabled", "true")
        .setting("xpack.security.authc.realms.saml.saml1.order", "4")
        .setting("xpack.security.authc.realms.saml.saml1.sp.logout", "https://kibana.org/logout")
        .setting("xpack.security.authc.realms.saml.saml1.idp.entity_id", "https://my-idp.org")
        .setting("xpack.security.authc.realms.saml.saml1.idp.metadata.path", "idp-docs-metadata.xml")
        .setting("xpack.security.authc.realms.saml.saml1.sp.entity_id", "https://kibana.org")
        .setting("xpack.security.authc.realms.saml.saml1.sp.acs", "https://kibana.org/api/security/saml/callback")
        .setting("xpack.security.authc.realms.saml.saml1.attributes.principal", "uid")
        .setting("xpack.security.authc.realms.saml.saml1.attributes.name", "urn:oid:2.5.4.3")
        .configFile("analysis/example_word_list.txt", Resource.fromFile(docsFile("src/test/cluster/config/analysis/example_word_list.txt")))
        .configFile(
            "analysis/hyphenation_patterns.xml",
            Resource.fromFile(docsFile("src/test/cluster/config/analysis/hyphenation_patterns.xml"))
        )
        .configFile("analysis/synonym.txt", Resource.fromFile(docsFile("src/test/cluster/config/analysis/synonym.txt")))
        .configFile("analysis/stemmer_override.txt", Resource.fromFile(docsFile("src/test/cluster/config/analysis/stemmer_override.txt")))
        .configFile("userdict_ja.txt", Resource.fromFile(docsFile("src/test/cluster/config/userdict_ja.txt")))
        .configFile("userdict_ko.txt", Resource.fromFile(docsFile("src/test/cluster/config/userdict_ko.txt")))
        .configFile("KeywordTokenizer.rbbi", Resource.fromFile(docsFile("src/test/cluster/config/KeywordTokenizer.rbbi")))
        .configFile("hunspell/en_US/en_US.aff", Resource.fromClasspath("indices/analyze/conf_dir/hunspell/en_US/en_US.aff"))
        .configFile("hunspell/en_US/en_US.dic", Resource.fromClasspath("indices/analyze/conf_dir/hunspell/en_US/en_US.dic"))
        .configFile("httpCa.p12", Resource.fromFile(docsFile("httpCa.p12")))
        .configFile("transport.p12", Resource.fromFile(docsFile("transport.p12")))
        .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
        .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
        .configFile("op-jwks.json", Resource.fromClasspath("oidc/op-jwks.json"))
        .configFile("idp-docs-metadata.xml", Resource.fromClasspath("idp/shibboleth-idp/metadata/idp-docs-metadata.xml"))
        .configFile("testClient.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/action/pki_delegation/testClient.crt"))
        .user(USER, PASS)
        .user("test_user", PASS)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    public DocsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        NamedXContentRegistry executableSectionRegistry = new NamedXContentRegistry(
            CollectionUtils.appendToCopy(
                ExecutableSection.DEFAULT_EXECUTABLE_CONTEXTS,
                new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("compare_analyzers"), CompareAnalyzers::parse)
            )
        );
        return ESClientYamlSuiteTestCase.createParameters(executableSectionRegistry);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected void afterIfFailed(List<Throwable> errors) {
        super.afterIfFailed(errors);
        String name = getTestName().split("=")[1];
        name = name.substring(0, name.length() - 1);
        name = name.replaceAll("/([^/]+)$", ".asciidoc:$1");
        logger.error(
            "This failing test was generated by documentation starting at {}. It may include many snippets. "
                + "See docs/README.asciidoc for an explanation of test generation.",
            name
        );
    }

    @Override
    protected boolean randomizeContentType() {
        return false;
    }

    @Override
    protected ClientYamlTestClient initClientYamlTestClient(
        final ClientYamlSuiteRestSpec restSpec,
        final RestClient restClient,
        final List<HttpHost> hosts
    ) {
        return new ClientYamlDocsTestClient(restSpec, restClient, hosts, this::getClientBuilderWithSniffedHosts);
    }

    @Before
    public void waitForRequirements() throws Exception {
        if (isCcrTest() || isGetLicenseTest() || isXpackInfoTest()) {
            ESRestTestCase.waitForActiveLicense(adminClient());
        }
    }

    private static boolean snapshotRepositoryPopulated;

    @Before
    public void populateSnapshotRepository() throws IOException {

        if (snapshotRepositoryPopulated) {
            return;
        }

        // The repository UUID is only created on the first write to the repo, so it may or may not exist when running the tests. However to
        // include the output from the put-repository and get-repositories APIs in the docs we must be sure whether the UUID is returned or
        // not, so we prepare by taking a snapshot first to ensure that the UUID really has been created.
        super.initClient();

        final Request putRepoRequest = new Request("PUT", "/_snapshot/test_setup_repo");
        putRepoRequest.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"my_backup_location\"}}");
        assertOK(adminClient().performRequest(putRepoRequest));

        final Request putSnapshotRequest = new Request("PUT", "/_snapshot/test_setup_repo/test_setup_snap");
        putSnapshotRequest.addParameter("wait_for_completion", "true");
        assertOK(adminClient().performRequest(putSnapshotRequest));

        final Request deleteSnapshotRequest = new Request("DELETE", "/_snapshot/test_setup_repo/test_setup_snap");
        assertOK(adminClient().performRequest(deleteSnapshotRequest));

        final Request deleteRepoRequest = new Request("DELETE", "/_snapshot/test_setup_repo");
        assertOK(adminClient().performRequest(deleteRepoRequest));

        snapshotRepositoryPopulated = true;
    }

    @After
    public void cleanup() throws Exception {
        if (isMachineLearningTest() || isTransformTest()) {
            ESRestTestCase.waitForPendingTasks(adminClient());
        }

        // check that there are no templates
        Request request = new Request("GET", "_cat/templates");
        request.addParameter("h", "name");
        String templates = EntityUtils.toString(adminClient().performRequest(request).getEntity());
        if (false == "".equals(templates)) {
            for (String template : templates.split("\n")) {
                if (isXPackTemplate(template)) continue;
                if ("".equals(template)) {
                    throw new IllegalStateException("empty template in templates list:\n" + templates);
                }
                throw new RuntimeException("Template " + template + " not cleared after test");
            }
        }
    }

    @Override
    protected boolean preserveSLMPoliciesUponCompletion() {
        return isSLMTest() == false;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return isILMTest() == false;
    }

    /**
     * Tests are themselves responsible for cleaning up templates, which speeds up build.
     */
    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    protected boolean isSLMTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("/slm/") || testName.contains("\\slm\\") || (testName.contains("\\slm/")) ||
        // TODO: Remove after backport of https://github.com/elastic/elasticsearch/pull/48705 which moves SLM docs to correct folder
            testName.contains("/ilm/") || testName.contains("\\ilm\\") || testName.contains("\\ilm/"));
    }

    protected boolean isILMTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("/ilm/") || testName.contains("\\ilm\\") || testName.contains("\\ilm/"));
    }

    protected boolean isMachineLearningTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("/ml/") || testName.contains("\\ml\\"));
    }

    protected boolean isTransformTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("/transform/") || testName.contains("\\transform\\"));
    }

    protected boolean isCcrTest() {
        String testName = getTestName();
        return testName != null && testName.contains("/ccr/");
    }

    protected boolean isGetLicenseTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("/get-license/") || testName.contains("\\get-license\\"));
    }

    protected boolean isXpackInfoTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("/info/") || testName.contains("\\info\\"));
    }

    private static final String USER_TOKEN = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));

    /**
     * All tests run as a an administrative user but use <code>es-shield-runas-user</code> to become a less privileged user.
     */
    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", USER_TOKEN).build();
    }

    /**
     * Deletes users after every test just in case any test adds any.
     */
    @After
    public void deleteUsers() throws Exception {
        ClientYamlTestResponse response = getAdminExecutionContext().callApi("security.get_user", emptyMap(), emptyList(), emptyMap());
        @SuppressWarnings("unchecked")
        Map<String, Object> users = (Map<String, Object>) response.getBody();
        for (String user : users.keySet()) {
            Map<?, ?> metadataMap = (Map<?, ?>) ((Map<?, ?>) users.get(user)).get("metadata");
            Boolean reserved = metadataMap == null ? null : (Boolean) metadataMap.get("_reserved");
            if (reserved == null || reserved == false) {
                logger.warn("Deleting leftover user {}", user);
                getAdminExecutionContext().callApi("security.delete_user", singletonMap("username", user), emptyList(), emptyMap());
            }
        }
    }

    /**
     * Re-enables watcher after every test just in case any test disables it.
     */
    @After
    public void reenableWatcher() throws Exception {
        if (isWatcherTest()) {
            assertBusy(() -> {
                ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", emptyMap(), emptyList(), emptyMap());
                String state = response.evaluate("stats.0.watcher_state");

                switch (state) {
                    case "stopped":
                        ClientYamlTestResponse startResponse = getAdminExecutionContext().callApi(
                            "watcher.start",
                            emptyMap(),
                            emptyList(),
                            emptyMap()
                        );
                        boolean isAcknowledged = startResponse.evaluate("acknowledged");
                        assertThat(isAcknowledged, is(true));
                        throw new AssertionError("waiting until stopped state reached started state");
                    case "stopping":
                        throw new AssertionError("waiting until stopping state reached stopped state to start again");
                    case "starting":
                        throw new AssertionError("waiting until starting state reached started state");
                    case "started":
                        // all good here, we are done
                        break;
                    default:
                        throw new AssertionError("unknown state[" + state + "]");
                }
            });
        }
    }

    protected boolean isWatcherTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("watcher/") || testName.contains("watcher\\"));
    }

    /**
     * Compares the results of running two analyzers against many random
     * strings. The goal is to figure out if two analyzers are "the same" by
     * comparing their results. This is far from perfect but should be fairly
     * accurate, especially for gross things like missing {@code decimal_digit}
     * token filters, and should be fairly fast because it compares a fairly
     * small number of tokens.
     */
    private static class CompareAnalyzers implements ExecutableSection {
        private static final ConstructingObjectParser<CompareAnalyzers, XContentLocation> PARSER = new ConstructingObjectParser<>(
            "test_analyzer",
            false,
            (a, location) -> {
                String index = (String) a[0];
                String first = (String) a[1];
                String second = (String) a[2];
                return new CompareAnalyzers(location, index, first, second);
            }
        );
        static {
            PARSER.declareString(constructorArg(), new ParseField("index"));
            PARSER.declareString(constructorArg(), new ParseField("first"));
            PARSER.declareString(constructorArg(), new ParseField("second"));
        }

        private static CompareAnalyzers parse(XContentParser parser) throws IOException {
            XContentLocation location = parser.getTokenLocation();
            CompareAnalyzers section = PARSER.parse(parser, location);
            assert parser.currentToken() == Token.END_OBJECT : "End of object required";
            parser.nextToken(); // throw out the END_OBJECT to conform with other ExecutableSections
            return section;
        }

        private final XContentLocation location;
        private final String index;
        private final String first;
        private final String second;

        private CompareAnalyzers(XContentLocation location, String index, String first, String second) {
            this.location = location;
            this.index = index;
            this.first = first;
            this.second = second;
        }

        @Override
        public XContentLocation getLocation() {
            return location;
        }

        @Override
        public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
            int size = 100;
            int maxLength = 15;
            List<String> testText = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                /*
                 * Build a string with a few unicode sequences separated by
                 * spaces. The unicode sequences aren't going to be of the same
                 * code page which is a shame because it makes the entire
                 * string less realistic. But this still provides a fairly
                 * nice string to compare.
                 */
                int spaces = between(0, 5);
                StringBuilder b = new StringBuilder((spaces + 1) * maxLength);
                b.append(randomRealisticUnicodeOfCodepointLengthBetween(1, maxLength));
                for (int t = 0; t < spaces; t++) {
                    b.append(' ');
                    b.append(randomRealisticUnicodeOfCodepointLengthBetween(1, maxLength));
                }
                testText.add(
                    b.toString()
                        // Don't look up stashed values
                        .replace("$", "\\$")
                );
            }
            Map<String, Object> body = Maps.newMapWithExpectedSize(2);
            body.put("analyzer", first);
            body.put("text", testText);
            ClientYamlTestResponse response = executionContext.callApi(
                "indices.analyze",
                singletonMap("index", index),
                singletonList(body),
                emptyMap()
            );
            Iterator<?> firstTokens = ((List<?>) response.evaluate("tokens")).iterator();
            body.put("analyzer", second);
            response = executionContext.callApi("indices.analyze", singletonMap("index", index), singletonList(body), emptyMap());
            Iterator<?> secondTokens = ((List<?>) response.evaluate("tokens")).iterator();

            Object previousFirst = null;
            Object previousSecond = null;
            while (firstTokens.hasNext()) {
                if (false == secondTokens.hasNext()) {
                    fail(Strings.format("""
                        %s has fewer tokens than %s. %s has [%s] but %s is out of tokens. \
                        %s's last token was [%s] and %s's last token was' [%s]
                        """, second, first, first, firstTokens.next(), second, first, previousFirst, second, previousSecond));
                }
                Map<?, ?> firstToken = (Map<?, ?>) firstTokens.next();
                Map<?, ?> secondToken = (Map<?, ?>) secondTokens.next();
                String firstText = (String) firstToken.get("token");
                String secondText = (String) secondToken.get("token");
                // Check the text and produce an error message with the utf8 sequence if they don't match.
                if (false == secondText.equals(firstText)) {
                    fail(Strings.format("""
                        text differs: %s was [%s] but %s was [%s]. In utf8 those are
                        %s and
                        %s
                        """, first, firstText, second, secondText, new BytesRef(firstText), new BytesRef(secondText)));
                }
                // Now check the whole map just in case the text matches but something else differs
                assertEquals(firstToken, secondToken);
                previousFirst = firstToken;
                previousSecond = secondToken;
            }
            if (secondTokens.hasNext()) {
                fail(Strings.format("""
                    %s has more tokens than %s. %s has [%s] but %s is out of tokens. \
                    %s's last token was [%s] and %s's last token was [%s]
                    """, second, first, second, secondTokens.next(), first, first, previousFirst, second, previousSecond));
            }
        }
    }
}
