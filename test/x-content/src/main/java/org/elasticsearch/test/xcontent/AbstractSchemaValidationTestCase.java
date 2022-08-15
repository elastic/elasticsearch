/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.xcontent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.AdditionalPropertiesValidator;
import com.networknt.schema.ItemsValidator;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.JsonValidator;
import com.networknt.schema.PropertiesValidator;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Test case for validating {@link ToXContent} objects against a json schema.
 */
public abstract class AbstractSchemaValidationTestCase<T extends ToXContent> extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    public final void testSchema() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SchemaValidatorsConfig config = new SchemaValidatorsConfig();
        JsonSchemaFactory factory = initializeSchemaFactory();

        Path p = getDataPath(getSchemaLocation() + getJsonSchemaFileName());
        logger.debug("loading schema from: [{}]", p);

        JsonSchema jsonSchema = factory.getSchema(mapper.readTree(Files.newInputStream(p)), config);

        // ensure the schema meets certain criteria like not empty, strictness
        assertTrue("found empty schema", jsonSchema.getValidators().size() > 0);
        assertTrue("schema lacks at least 1 required field", jsonSchema.hasRequiredValidator());
        assertSchemaStrictness(jsonSchema.getValidators().values(), jsonSchema.getSchemaPath());

        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            BytesReference xContent = XContentHelper.toXContent(createTestInstance(), XContentType.JSON, getToXContentParams(), false);
            JsonNode jsonTree = mapper.readTree(xContent.streamInput());

            Set<ValidationMessage> errors = jsonSchema.validate(jsonTree);
            assertThat("Schema validation failed for: " + jsonTree.toPrettyString(), errors, is(empty()));
        }
    }

    /**
     * Creates a random instance to use in the schema tests.
     * Override this method to return the random instance that you build
     * which must implement {@link ToXContent}.
     */
    protected abstract T createTestInstance();

    /**
     * Return the filename of the schema file used for testing.
     */
    protected abstract String getJsonSchemaFileName();

    /**
     * Params that have to be provided when calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)}
     */
    protected ToXContent.Params getToXContentParams() {
        return ToXContent.EMPTY_PARAMS;
    }

    /**
     * Root folder for all schema files.
     */
    protected String getSchemaLocation() {
        return "/rest-api-spec/schema/";
    }

    /**
     * Version of the Json Schema Spec to be used by the test.
     */
    protected SpecVersion.VersionFlag getSchemaVersion() {
        return SpecVersion.VersionFlag.V7;
    }

    /**
     * Loader for the schema factory.
     *
     * Uses the ootb factory but replaces the loader for sub schema's stored on the file system.
     */
    private JsonSchemaFactory initializeSchemaFactory() {
        JsonSchemaFactory factory = JsonSchemaFactory.builder(JsonSchemaFactory.getInstance(getSchemaVersion())).uriFetcher(uri -> {
            String fileName = uri.toString().substring(uri.getScheme().length() + 1);
            Path path = getDataPath(getSchemaLocation() + fileName);
            logger.debug("loading sub-schema [{}] from: [{}]", uri, path);
            return Files.newInputStream(path);
        }, "file").build();

        return factory;
    }

    /**
     * Enforce that the schema as well as all sub schemas define all properties.
     *
     * This uses an implementation detail of the schema validation library: If
     * strict validation is turned on (`"additionalProperties": false`), the schema
     * validator injects an instance of AdditionalPropertiesValidator.
     *
     * The check loops through the validator tree and checks for instances of
     * AdditionalPropertiesValidator. If it is absent at expected places the test fails.
     *
     * Note: we might not catch all places, but at least it works for nested objects and
     * array items.
     */
    private void assertSchemaStrictness(Collection<JsonValidator> validatorSet, String path) {
        boolean additionalPropertiesValidatorFound = false;
        boolean subSchemaFound = false;

        for (JsonValidator validator : validatorSet) {
            if (validator instanceof PropertiesValidator propertiesValidator) {
                subSchemaFound = true;
                for (Entry<String, JsonSchema> subSchema : propertiesValidator.getSchemas().entrySet()) {
                    assertSchemaStrictness(subSchema.getValue().getValidators().values(), propertiesValidator.getSchemaPath());
                }
            } else if (validator instanceof ItemsValidator itemValidator) {
                if (itemValidator.getSchema() != null) {
                    assertSchemaStrictness(itemValidator.getSchema().getValidators().values(), itemValidator.getSchemaPath());
                }
                if (itemValidator.getTupleSchema() != null) {
                    for (JsonSchema subSchema : itemValidator.getTupleSchema()) {
                        assertSchemaStrictness(subSchema.getValidators().values(), itemValidator.getSchemaPath());
                    }
                }
            } else if (validator instanceof AdditionalPropertiesValidator) {
                additionalPropertiesValidatorFound = true;
            }
        }

        // if not a leaf, additional property strictness must be set
        assertTrue(
            "the schema must have additional properties set to false (\"additionalProperties\": false) in all (sub) schemas, "
                + "missing at least for path: "
                + path,
            subSchemaFound == false || additionalPropertiesValidatorFound
        );
    }
}
