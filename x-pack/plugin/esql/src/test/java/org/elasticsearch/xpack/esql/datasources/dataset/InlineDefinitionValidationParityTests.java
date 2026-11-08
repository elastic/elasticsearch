/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasources.DatasetReference;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.DataSourceLimits.MAX_DESCRIPTION_LENGTH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A definition a query carries is held to exactly what a registration is held to. These tests put one body through
 * both doors — {@link DatasetService#validatePutDataset}, which a {@code PUT} goes through, and
 * {@link DatasetDefinitionValidator#validateInline}, which {@code DATASET(...)} goes through — and require the same
 * exception, of the same class, with the same words. A second validator would pass a weaker version of each of these
 * and nothing else would notice.
 */
public class InlineDefinitionValidationParityTests extends ESTestCase {

    private static final String DATA_SOURCE = "parent";
    private static final String RESOURCE = "s3://bucket/*.csv";

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void startClusterService() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void stopClusterService() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testShadowedSecretIsRefusedTheSameWayByBothDoors() throws IOException {
        assertSameRefusal(definition(Map.of("settings", Map.of("secret_key", "override"))), "shadows a secret data-source setting");
    }

    public void testUnknownSettingIsRefusedTheSameWayByBothDoors() throws IOException {
        assertSameRefusal(definition(Map.of("settings", Map.of("bogus", 1))), "unknown setting [bogus]");
    }

    public void testUndeclarableMappingTypeIsRefusedTheSameWayByBothDoors() throws IOException {
        Map<String, Object> mappings = Map.of("properties", Map.of("a", Map.of("type", "nested")));
        assertSameRefusal(definition(Map.of("mappings", mappings)), "unsupported declared type [nested] for column [a]");
    }

    public void testStrictMappingWithNoColumnsIsRefusedTheSameWayByBothDoors() throws IOException {
        Map<String, Object> mappings = Map.of("dynamic", false, "properties", Map.of());
        assertSameRefusal(definition(Map.of("mappings", mappings)), "[dynamic: false] requires at least one declared column");
    }

    /**
     * The positive control for the four above: a body both doors accept has to yield the same dataset, or an
     * assertion that the refusals match says nothing about what is accepted.
     */
    public void testAnAcceptedDefinitionYieldsTheSameDatasetThroughBothDoors() throws IOException {
        Map<String, Object> body = definition(
            Map.of("settings", Map.of("accepted", "value"), "mappings", Map.of("properties", Map.of("a", Map.of("type", "long"))))
        );

        Dataset registered = service().validatePutDataset(project(), request(body));
        Dataset inline = validator().validateInline(project(), inline(body)).dataset();

        assertThat(inline, equalTo(registered));
    }

    /** The data source is the function's first argument, so a definition naming one as well is ambiguous. */
    public void testDefinitionNamingItsOwnDataSourceIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(
            DATA_SOURCE,
            Map.of("resource", RESOURCE, "data_source", "somewhere_else")
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("takes its data source from its first argument"));
    }

    /** A field a registration body has no slot for is refused by the registration parser, which this reuses. */
    public void testDefinitionWithAnUnknownFieldIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(DATA_SOURCE, Map.of("resource", RESOURCE, "formt", "csv"));
        Exception e = expectThrows(Exception.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("[formt]"));
    }

    /** A definition with no resource cannot name anything to read; the registration body's own requirement. */
    public void testDefinitionWithNoResourceIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(DATA_SOURCE, Map.of("settings", Map.of("accepted", "value")));
        Exception e = expectThrows(Exception.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("resource"));
    }

    /**
     * An <i>empty</i> resource, which is the one defect only the request's own {@code validate()} catches: the
     * parser is satisfied by the field being present, and the shared validator never looks at it.
     * <p>
     * This is deliberately not an {@link #assertSameRefusal} case, because the two doors are genuinely asymmetric
     * here and that assertion would fail for the wrong reason. A {@code PUT} has {@code validate()} run for it by
     * the transport layer before {@code validatePutDataset} is ever called, so the registered door does not refuse
     * this one itself; the inline door has nothing in front of it and so runs {@code validate()} on the request it
     * assembles. Deleting that call leaves the whole package green, which is what this pins.
     */
    public void testDefinitionWithAnEmptyResourceIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(DATA_SOURCE, Map.of("resource", ""));
        Exception e = expectThrows(Exception.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("dataset resource is missing or empty"));
    }

    /** The description bound is the request's too, and reaches an inline definition by the same one call. */
    public void testDefinitionWithTooLongADescriptionIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(
            DATA_SOURCE,
            Map.of("resource", RESOURCE, "description", randomAlphaOfLength(MAX_DESCRIPTION_LENGTH + 1))
        );
        Exception e = expectThrows(Exception.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("dataset description is too large"));
    }

    private void assertSameRefusal(Map<String, Object> body, String expected) throws IOException {
        Exception registered = expectThrows(Exception.class, () -> service().validatePutDataset(project(), request(body)));
        Exception inline = expectThrows(Exception.class, () -> validator().validateInline(project(), inline(body)));

        assertThat(registered.getMessage(), containsString(expected));
        assertThat(inline.getClass(), equalTo(registered.getClass()));
        assertThat(inline.getMessage(), equalTo(registered.getMessage()));
    }

    /** A registration body: the resource plus whatever the case under test adds. */
    private static Map<String, Object> definition(Map<String, Object> extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("resource", RESOURCE);
        body.putAll(extra);
        return body;
    }

    /**
     * The body as a {@code PUT} request, read by the REST parser rather than assembled field by field. Registered
     * under the data source's own name, which is the name an inline read takes, so the two datasets are comparable
     * field for field; the index and data-source namespaces are separate, so that name is registrable.
     */
    private static PutDatasetAction.Request request(Map<String, Object> body) throws IOException {
        Map<String, Object> withDataSource = new LinkedHashMap<>(body);
        withDataSource.put("data_source", DATA_SOURCE);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(withDataSource);
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
            ) {
                return PutDatasetAction.Request.fromXContent(
                    parser,
                    TimeValue.timeValueSeconds(30),
                    TimeValue.timeValueSeconds(30),
                    DATA_SOURCE
                );
            }
        }
    }

    private static DatasetReference.Inline inline(Map<String, Object> body) {
        return new DatasetReference.Inline(DATA_SOURCE, body);
    }

    private DatasetService service() {
        return new DatasetService(clusterService, Map.of("strict", new UnknownKeyRejectingValidator()));
    }

    private static DatasetDefinitionValidator validator() {
        return new DatasetDefinitionValidator(Map.of("strict", new UnknownKeyRejectingValidator()));
    }

    /**
     * The same unknown-key contract the real file validators have, so a setting outside the vocabulary is refused
     * rather than passed through. {@code DatasetServiceTests} uses the same shape for the same reason.
     */
    private static class UnknownKeyRejectingValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "strict";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            return Map.of();
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            for (String key : datasetSettings.keySet()) {
                if (key.equals("accepted") == false) {
                    ValidationException e = new ValidationException();
                    e.addValidationError("unknown setting [" + key + "]");
                    throw e;
                }
            }
            return new HashMap<>(datasetSettings);
        }
    }

    private static ProjectMetadata project() {
        DataSource parent = new DataSource(
            DATA_SOURCE,
            "strict",
            null,
            Map.of("secret_key", new DataSourceSetting("s3cr3t", true), "accepted", new DataSourceSetting("parent-value", false))
        );
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of(DATA_SOURCE, parent)))
            .build();
    }
}
