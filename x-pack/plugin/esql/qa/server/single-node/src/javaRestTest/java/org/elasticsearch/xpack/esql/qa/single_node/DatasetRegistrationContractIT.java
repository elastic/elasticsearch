/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.fixtures.RegistrationContract;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * What {@code PUT /_query/dataset} refuses, and what it says when it does.
 *
 * <p>Registration performs no I/O. The endpoint cannot open the object, so everything it can decide it
 * decides from the settings alone -- which makes "which settings can be refused, and with what message"
 * the whole of its contract, and the message the part a user acts on. A setting that cannot be decided
 * up front is a different case: it is accepted here and fails at query time, and that half is not
 * covered by this suite.
 *
 * <p>Asserting the MESSAGE rather than the status is the point. A case that expects any 400 passes when
 * a setting it never varied is refused for an unrelated reason -- a mistyped key, a value rejected by a
 * different bound -- and the two are the same green tick everywhere downstream. That is not
 * hypothetical: the case list here includes two values of one setting whose refusals come from
 * different branches, a parse and a range check, which a status-only assertion cannot tell apart.
 *
 * <p>The cases come from {@code registration-contract.properties} rather than from this file, so adding
 * one is a line of declaration and the messages sit together where they can be re-read against the
 * components that emit them. Each case names the emitting symbol for exactly that reason.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DatasetRegistrationContractIT extends ESRestTestCase {

    private static final Path FIXTURE_DIR = initFixtureDir();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(FIXTURE_DIR, config -> {}, false);

    private static final String SHARED_DS_NAME = "registration_contract_ds";

    private final RegistrationContract.Case contractCase;

    public DatasetRegistrationContractIT(String name, RegistrationContract.Case contractCase) {
        this.contractCase = contractCase;
    }

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> parameters() {
        List<Object[]> cases = new ArrayList<>();
        for (RegistrationContract.Case declared : RegistrationContract.get().cases()) {
            cases.add(new Object[] { declared.name(), declared });
        }
        return cases;
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @Before
    public void ensureSharedDs() throws IOException {
        DatasetRegistry.ensureDataSource(client(), SHARED_DS_NAME, "local", Map.of());
    }

    @AfterClass
    public static void teardown() {
        if (Build.current().isSnapshot() == false) {
            return;
        }
        try {
            DatasetRegistry.cleanup(client());
        } catch (Exception e) {
            // Teardown of a suite that registers nothing successfully; a failure here says nothing about
            // the contract and must not mask the case that did fail.
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /** This suite creates no indices, so the per-test wipe ESRestTestCase issues is pure overhead. */
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testTheRegistrationIsRefusedWithItsDeclaredMessage() throws IOException {
        assumeTrue("this case is about the query, not the registration", contractCase.outcome() == RegistrationContract.Outcome.REFUSED);
        assumeFalse(
            "blocked on " + contractCase.blockedBy() + " -- this case asserts behaviour the product does not have yet",
            contractCase.blocked()
        );
        String dataset = "contract_" + contractCase.name();
        // toUri(), not toString(): the endpoint requires a file:// URI, and a bare path is refused for
        // THAT rather than for the setting -- a 400 either way, which is what makes the message the
        // assertion. It also decides the case: the format is resolved from the resource's extension, and
        // an unresolved format leaves the format-scoped keys out of the accepted set, so the setting under
        // test comes back as an unknown key instead of an out-of-range one.
        String resource = resourceFor(contractCase);

        ResponseException refused = expectThrows(
            ResponseException.class,
            contractCase.settings()
                + " was accepted at registration. Either the validator stopped checking it, or it now "
                + "defers to query time -- which is a contract change rather than a passing case.",
            () -> DatasetRegistry.putDataset(client(), dataset, SHARED_DS_NAME, resource, Map.copyOf(contractCase.settings()))
        );

        assertThat(
            "the refusal must be about the settings the case varied",
            refused.getResponse().getStatusLine().getStatusCode(),
            equalTo(400)
        );
        if (contractCase.absent() != null) {
            assertThat(
                "the refusal carries [" + contractCase.absent() + "], which this case exists to say it must not",
                refused.getMessage(),
                not(containsString(contractCase.absent()))
            );
        }
        assertThat(
            "refused, but not for the declared reason -- a status-only check would have passed here. "
                + "The message is emitted by ["
                + contractCase.emitter()
                + "]; if it moved, update registration-contract.properties from the code rather than the other way round.",
            refused.getMessage(),
            containsString(contractCase.message())
        );
    }

    /**
     * The other half of the no-I/O promise: registration cannot open the object, so a setting it cannot
     * decide about is ACCEPTED here and fails when the reader finally looks.
     *
     * <p>Both halves are assertions. If the PUT starts refusing one of these, registration has acquired
     * I/O and the contract has changed; if the query stops failing, the reader has started tolerating
     * something it used to reject. The message after the query is what a user actually sees, and it is
     * the only place this contract is visible at all -- no registration-time test can reach it.
     */
    public void testTheRegistrationIsAcceptedAndTheQueryFailsWithItsDeclaredMessage() throws IOException {
        assumeTrue(
            "this case is about the registration, not the query",
            contractCase.outcome() == RegistrationContract.Outcome.QUERY_FAILS
        );
        assumeFalse(
            "blocked on " + contractCase.blockedBy() + " -- this case asserts behaviour the product does not have yet",
            contractCase.blocked()
        );
        String dataset = "contract_" + contractCase.name();
        String resource = resourceFor(contractCase);

        DatasetRegistry.putDataset(client(), dataset, SHARED_DS_NAME, resource, Map.copyOf(contractCase.settings()));

        Request query = new Request("POST", "/_query");
        query.setJsonEntity("{\"query\": \"" + String.format(Locale.ROOT, contractCase.query(), dataset) + "\"}");
        ResponseException failed = expectThrows(
            ResponseException.class,
            contractCase.settings()
                + " registered and then QUERIED CLEANLY. Either the reader now tolerates it, or the bytes "
                + "stopped disagreeing with the settings -- either way this case is no longer testing what it says.",
            () -> client().performRequest(query)
        );

        assertThat(
            "the query failed, but not for the declared reason. The message is emitted by ["
                + contractCase.emitter()
                + "]; if it moved, update registration-contract.properties from the code rather than the other way round.",
            failed.getMessage(),
            containsString(contractCase.message())
        );
    }

    /**
     * The resource as the case wants it written.
     *
     * <p>A URI by default, because that is what a correct request carries. A case may ask for the bare
     * filesystem path instead, which is what a user writes by mistake -- and the mistake is the subject
     * of the case rather than a setup detail, so the declaration says which form it wants rather than
     * the suite guessing from the string.
     */
    private static String resourceFor(RegistrationContract.Case declared) {
        java.nio.file.Path file = FIXTURE_DIR.resolve(declared.resource());
        return declared.rawPath() ? file.toString() : file.toUri().toString();
    }

    /**
     * A file the registration never opens.
     *
     * <p>Every case here is refused before any I/O happens, so the bytes are irrelevant -- but the path
     * must exist and sit under the cluster's allowed paths, or the refusal could come from path
     * validation instead of from the setting, and the suite would assert its own message against the
     * wrong branch.
     */
    private static Path initFixtureDir() {
        try {
            Path dir = Files.createTempDirectory("registration-contract-");
            Files.writeString(dir.resolve("simple.csv"), "a,b\n1,foo\n2,bar\n");
            // CSV bytes under a name that implies no format, so an explicit `format` decides what the
            // reader is told to expect and nothing at registration can contradict it.
            Files.writeString(dir.resolve("noext"), "a,b\n1,foo\n2,bar\n");
            return dir;
        } catch (IOException e) {
            throw new AssertionError("could not lay down the registration-contract fixture", e);
        }
    }
}
