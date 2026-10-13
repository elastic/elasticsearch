/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.support.DlsLookupResolver;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end coverage of DLS lookups declared by implicitly granted privileges:
 * <ol>
 *   <li>An {@link ImplicitPrivilegesProvider} sees a role's {@code kibana-.kibana / ml_read} grant on {@code space:<name>}
 *       resources and synthesizes {@code read} on {@code ml_results} with a templated DLS query. The template reads
 *       {@code _lookup.ml_jobs}, which the query's {@code lookups} section binds to the {@code ml_job_ids} resolver type with the
 *       user's spaces as params.</li>
 *   <li>The {@link SecurityExtension} registers a {@link DlsLookupResolver} for {@code ml_job_ids} that searches {@code ml_jobs}
 *       under the security origin for jobs visible in those spaces.</li>
 *   <li>When the user searches {@code ml_results}, the coordinating node resolves the lookup once, carries the job ids on the
 *       request, and every shard renders the same DLS filter.</li>
 * </ol>
 * The users hold only the application privilege; all access to {@code ml_results} flows through the implicit privilege.
 */
public class DlsLookupIntegTests extends SecurityIntegTestCase {

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String ML_READ_PRIVILEGE = "ml_read";
    private static final String ML_READ_ACTION = "ml:read";
    private static final String SPACE_RESOURCE_PREFIX = "space:";
    private static final String ML_JOBS_INDEX = "ml_jobs";
    private static final String ML_RESULTS_INDEX = "ml_results";
    private static final String LOOKUP_TYPE = "ml_job_ids";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(LocalStateSecurity.class);
        plugins.add(LocalStateWithDlsLookups.class);
        plugins.add(MustachePlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Class<?> xpackPluginClass() {
        return LocalStateWithDlsLookups.class;
    }

    @Before
    public void setUpPrivilegesAndData() {
        final PutPrivilegesRequest putPrivileges = new PutPrivilegesRequest();
        putPrivileges.setPrivileges(
            List.of(new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, ML_READ_PRIVILEGE, Set.of(ML_READ_ACTION), Map.of()))
        );
        client().execute(PutPrivilegesAction.INSTANCE, putPrivileges).actionGet();

        assertAcked(indicesAdmin().prepareCreate(ML_JOBS_INDEX).setMapping("ml_job_id", "type=keyword", "namespaces", "type=keyword"));
        assertAcked(
            indicesAdmin().prepareCreate(ML_RESULTS_INDEX)
                // several shards so that shard-level authorizations, possibly on other nodes, must reuse the resolved values
                .setSettings(Settings.builder().put("index.number_of_shards", 3))
                .setMapping("ml_job_id", "type=keyword", "value", "type=keyword")
        );
        // j1 and j2 are in "myspace"; j3 is in "other"; j4 is in no space.
        indexJob("j1", List.of("myspace"));
        indexJob("j2", List.of("myspace", "other"));
        indexJob("j3", List.of("other"));
        indexJob("j4", List.of());
        // r1a, r1b -> j1; r2 -> j2; r3 -> j3; r4 -> j4
        indexResult("r1a", "j1");
        indexResult("r1b", "j1");
        indexResult("r2", "j2");
        indexResult("r3", "j3");
        indexResult("r4", "j4");
        MlJobIdsLookupResolver.reset();
    }

    public void testLookupScopesResultsToUserSpaceAndResolvesOncePerRequest() {
        createUserWithSpace("myspace_user", "myspace_role", "myspace");
        createUserWithSpace("other_user", "other_role", "other");

        assertResponse(clientFor("myspace_user").prepareSearch(ML_RESULTS_INDEX).setSize(50), response -> {
            assertThat(response.getFailedShards(), equalTo(0));
            assertThat(hitIds(response.getHits().getHits()), containsInAnyOrder("r1a", "r1b", "r2"));
        });
        // Every shard-level action of the search is authorized again, on whichever node holds the shard, yet the lookup is
        // resolved exactly once because the coordinating node carries the values on the request.
        assertThat(MlJobIdsLookupResolver.invocations(), contains(new Invocation("myspace_user", Set.of("myspace"))));

        assertResponse(clientFor("other_user").prepareSearch(ML_RESULTS_INDEX).setSize(50), response -> {
            assertThat(response.getFailedShards(), equalTo(0));
            assertThat(hitIds(response.getHits().getHits()), containsInAnyOrder("r2", "r3"));
        });
        assertThat(
            MlJobIdsLookupResolver.invocations(),
            contains(new Invocation("myspace_user", Set.of("myspace")), new Invocation("other_user", Set.of("other")))
        );

        // A new request resolves again: values are scoped to a request, never cached across requests here.
        assertResponse(
            clientFor("myspace_user").prepareSearch(ML_RESULTS_INDEX).setQuery(QueryBuilders.termQuery("ml_job_id", "j3")),
            response -> assertThat(response.getHits().getTotalHits().value(), equalTo(0L))
        );
        assertThat(MlJobIdsLookupResolver.invocations().size(), equalTo(3));
    }

    public void testImplicitPrivilegeWithLookupIsReportedByGetUserPrivileges() {
        createUserWithSpace("reporter", "reporter_role", "myspace");

        final GetUserPrivilegesResponse response = new GetUserPrivilegesRequestBuilder(clientFor("reporter")).username("reporter").get();
        final GetUserPrivilegesResponse.Indices implicit = response.getIndexPrivileges()
            .stream()
            .filter(idx -> idx.getIndices().contains(ML_RESULTS_INDEX))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected implicit privilege on " + ML_RESULTS_INDEX + " in " + response));
        assertThat(implicit.getPrivileges(), contains("read"));
        final List<String> queries = implicit.getQueries().stream().map(BytesReference::utf8ToString).toList();
        assertThat(queries, contains(TestMlImplicitPrivilegesProvider.dlsQuery(Set.of("myspace")).utf8ToString()));
        assertThat(queries.get(0), containsString("\"lookups\":{\"ml_jobs\":{\"type\":\"" + LOOKUP_TYPE + "\""));
    }

    public void testRoleDefinitionsCannotDeclareLookups() {
        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> new PutRoleRequestBuilder(client()).name("handwritten")
                .addIndices(
                    new String[] { ML_RESULTS_INDEX },
                    new String[] { "read" },
                    null,
                    null,
                    TestMlImplicitPrivilegesProvider.dlsQuery(Set.of("myspace")),
                    false
                )
                .get()
        );
        assertThat(e.getMessage(), containsString("failed to parse field 'query' for indices [" + ML_RESULTS_INDEX + "]"));
        assertThat(e.getCause().getMessage(), containsString("[lookups] is not supported in a role query"));
    }

    private void indexJob(String jobId, List<String> namespaces) {
        prepareIndex(ML_JOBS_INDEX).setId(jobId).setSource("ml_job_id", jobId, "namespaces", namespaces).setRefreshPolicy(IMMEDIATE).get();
    }

    private void indexResult(String id, String jobId) {
        prepareIndex(ML_RESULTS_INDEX).setId(id).setSource("ml_job_id", jobId, "value", "v-" + id).setRefreshPolicy(IMMEDIATE).get();
    }

    private void createUserWithSpace(String username, String roleName, String space) {
        final PutRoleRequestBuilder putRole = new PutRoleRequestBuilder(client()).name(roleName);
        putRole.request()
            .addApplicationPrivileges(
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges(ML_READ_PRIVILEGE)
                    .resources(SPACE_RESOURCE_PREFIX + space)
                    .build()
            );
        putRole.get();
        new PutUserRequestBuilder(client()).username(username)
            .password(TEST_PASSWORD_SECURE_STRING, getFastStoredHashAlgoForTests())
            .roles(roleName)
            .get();
    }

    private Client clientFor(String username) {
        return client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, TEST_PASSWORD_SECURE_STRING)));
    }

    private static Set<String> hitIds(SearchHit[] hits) {
        return Arrays.stream(hits).map(SearchHit::getId).collect(Collectors.toSet());
    }

    /** One resolver invocation: which user it ran for and which spaces it was asked about. */
    record Invocation(String principal, Set<String> spaces) {}

    public static class LocalStateWithDlsLookups extends LocalStateSecurity {
        public LocalStateWithDlsLookups(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
        }

        @Override
        protected List<SecurityExtension> securityExtensions() {
            return List.of(new TestDlsLookupExtension());
        }
    }

    /**
     * Registers both halves of the feature: the provider that emits the lookup-bearing implicit privilege, and the resolver for
     * the lookup type that privilege references.
     */
    static class TestDlsLookupExtension implements SecurityExtension {
        @Override
        public String extensionName() {
            return "test-dls-lookup-extension";
        }

        @Override
        public List<ImplicitPrivilegesProvider> getImplicitPrivilegesProviders(SecurityComponents components) {
            return List.of(new TestMlImplicitPrivilegesProvider());
        }

        @Override
        public Map<String, DlsLookupResolver> getDlsLookupResolvers(SecurityComponents components) {
            return Map.of(LOOKUP_TYPE, new MlJobIdsLookupResolver(components.client()));
        }
    }

    /**
     * Grants {@code read} on {@code ml_results} to roles holding {@code ml:read} in the Kibana application, filtered to results
     * of jobs in the granted spaces. The spaces are known here, so they become the lookup's params; the job ids are not, so they
     * are left to the resolver.
     */
    static class TestMlImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

        @Override
        public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
            Collection<ResolvedApplicationPrivilege> applicationPrivileges
        ) {
            final Set<String> spaces = new TreeSet<>();
            for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
                final ApplicationPrivilege privilege = resolved.privilege();
                if (KIBANA_APPLICATION.equals(privilege.getApplication()) && privilege.predicate().test(ML_READ_ACTION)) {
                    resolved.resources()
                        .stream()
                        .filter(r -> r.startsWith(SPACE_RESOURCE_PREFIX))
                        .map(r -> r.substring(SPACE_RESOURCE_PREFIX.length()))
                        .forEach(spaces::add);
                }
            }
            if (spaces.isEmpty()) {
                return List.of();
            }
            return List.of(
                RoleDescriptor.IndicesPrivileges.builder().indices(ML_RESULTS_INDEX).privileges("read").query(dlsQuery(spaces)).build()
            );
        }

        static BytesReference dlsQuery(Set<String> spaces) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.startObject();
                builder.startObject("template");
                builder.field("source", "{\"terms\":{\"ml_job_id\":{{#toJson}}_lookup.ml_jobs{{/toJson}}}}");
                builder.endObject();
                builder.startObject("lookups");
                builder.startObject("ml_jobs");
                builder.field("type", LOOKUP_TYPE);
                builder.startObject("params");
                builder.array("spaces", new TreeSet<>(spaces).toArray(String[]::new));
                builder.endObject();
                builder.endObject();
                builder.endObject();
                builder.endObject();
                return BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Resolves {@code ml_job_ids} by searching {@code ml_jobs} for jobs whose {@code namespaces} intersect the requested spaces.
     * Runs under the security origin, so the translation is not itself subject to whatever DLS applies to the caller. Records
     * every invocation so the tests can pin the once-per-request guarantee.
     */
    static class MlJobIdsLookupResolver implements DlsLookupResolver {

        private static final List<Invocation> INVOCATIONS = new CopyOnWriteArrayList<>();

        private final Client client;

        MlJobIdsLookupResolver(Client client) {
            this.client = client;
        }

        static void reset() {
            INVOCATIONS.clear();
        }

        static List<Invocation> invocations() {
            return List.copyOf(INVOCATIONS);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void resolve(Map<String, Object> params, Subject effectiveSubject, ActionListener<Object> listener) {
            final Set<String> spaces = new HashSet<>((List<String>) params.get("spaces"));
            INVOCATIONS.add(new Invocation(effectiveSubject.getUser().principal(), spaces));
            final SearchRequest request = new SearchRequest(ML_JOBS_INDEX).source(
                SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.termsQuery("namespaces", spaces.toArray(String[]::new)))
                    .fetchSource(new String[] { "ml_job_id" }, null)
                    .size(1000)
            );
            ClientHelper.executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                TransportSearchAction.TYPE,
                request,
                listener.delegateFailureAndWrap((delegate, response) -> {
                    // Sorted so the rendered query, and with it the request cache key, is stable
                    final Set<String> jobIds = new TreeSet<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        jobIds.add(String.valueOf(hit.getSourceAsMap().get("ml_job_id")));
                    }
                    delegate.onResponse(new ArrayList<>(jobIds));
                })
            );
        }
    }
}
