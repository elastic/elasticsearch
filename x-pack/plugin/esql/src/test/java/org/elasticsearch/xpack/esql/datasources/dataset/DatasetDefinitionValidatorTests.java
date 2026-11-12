/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetReference;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.DataSourceLimits.MAX_DESCRIPTION_LENGTH;
import static org.hamcrest.Matchers.containsString;

/**
 * The checks that only reach a definition carried by a query. What a registration is held to is the same code and is
 * covered by {@code DatasetServiceTests}; these are the ones with no transport layer in front of them.
 */
public class DatasetDefinitionValidatorTests extends ESTestCase {

    private static final String DATA_SOURCE = "parent";
    private static final String RESOURCE = "s3://bucket/*.csv";

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
     * An <i>empty</i> resource: the parser is satisfied by the field being present and the shared validator never
     * looks at it, so only the request's own {@code validate()} catches this. A registration has that run for it by
     * the transport layer; an inline definition has nothing in front of it, so {@code validateInline} runs it. Delete
     * that call and this is the only test that reddens.
     */
    public void testDefinitionWithAnEmptyResourceIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(DATA_SOURCE, Map.of("resource", ""));
        Exception e = expectThrows(Exception.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("dataset resource is missing or empty"));
    }

    /** The description limit is the request's too, and reaches an inline definition by the same one call. */
    public void testDefinitionWithTooLongADescriptionIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(
            DATA_SOURCE,
            Map.of("resource", RESOURCE, "description", randomAlphaOfLength(MAX_DESCRIPTION_LENGTH + 1))
        );
        Exception e = expectThrows(Exception.class, () -> validator().validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("dataset description is too large"));
    }

    /**
     * A data source whose type nothing can validate. Reachable because the validators come from the plugins a node
     * loaded while the data source lives in cluster state: a node missing the plugin refuses the read rather than
     * binding a definition nothing checked.
     */
    public void testADataSourceWithNoValidatorIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(DATA_SOURCE, Map.of("resource", RESOURCE));
        DatasetDefinitionValidator noValidators = new DatasetDefinitionValidator(Map.of());

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> noValidators.validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("no validator registered for data source type [strict]"));
    }

    /**
     * The second shadow check, over the settings the validator <i>returned</i> rather than those the request sent.
     * A validator that invents a key naming one of the source's secrets would otherwise put it on the Dataset, where
     * it reads as a dataset-level override of a credential.
     */
    public void testAValidatorThatInventsAShadowingSettingIsRefused() {
        DatasetReference.Inline inline = new DatasetReference.Inline(DATA_SOURCE, Map.of("resource", RESOURCE));
        DatasetDefinitionValidator inventing = new DatasetDefinitionValidator(Map.of("strict", new SecretInventingValidator()));

        ValidationException e = expectThrows(ValidationException.class, () -> inventing.validateInline(project(), inline));
        assertThat(e.getMessage(), containsString("dataset setting [secret_key] shadows a secret data-source setting"));
    }

    /** Returns a key the request never sent, which only the post-validator check can catch. */
    private static class SecretInventingValidator implements DataSourceValidator {
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
            Map<String, Object> invented = new HashMap<>(datasetSettings);
            invented.put("secret_key", "mine-not-the-sources");
            return invented;
        }
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
