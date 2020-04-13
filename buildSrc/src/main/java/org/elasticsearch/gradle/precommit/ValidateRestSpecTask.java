/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.precommit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaException;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Validates the REST specification against a schema. There should only be a single location for the schema, but any project can contribute
 * to the REST specification.
 */
public class ValidateRestSpecTask extends PrecommitTask {

    private static final String SCHEMA_PROJECT = ":rest-api-spec";
    private static final String DOUBLE_STARS = "**"; // checkstyle thinks this is an empty javadoc statement, so string concat instead
    private static final String JSON_SPEC_PATTERN_INCLUDE = DOUBLE_STARS + "/rest-api-spec/api/" + DOUBLE_STARS + "/*.json";
    private static final String JSON_SPEC_PATTERN_EXCLUDE = DOUBLE_STARS + "/_common.json";
    private static final String JSON_SCHEMA_PATTERN = DOUBLE_STARS + "/schema.json";
    private static final ObjectMapper mapper = new ObjectMapper();
    private Set<String> ignore = new HashSet<>();

    @SkipWhenEmpty
    @InputFiles
    public FileTree getInputDir() {
        // if null results in in NO-SOURCE
        return Util.getJavaTestAndMainSourceResources(
            getProject(),
            Util.filterByPatterns(
                getPatternSetFactory().create(),
                Collections.singletonList(JSON_SPEC_PATTERN_INCLUDE),
                Collections.singletonList(JSON_SPEC_PATTERN_EXCLUDE)
            )
        );
    }

    @Input
    @Optional
    public Set<String> getIgnore() {
        return ignore;
    }

    public void ignore(String... ignore) {
        this.ignore.addAll(Arrays.asList(ignore));
    }

    @Inject
    protected Factory<PatternSet> getPatternSetFactory() {
        throw new UnsupportedOperationException();
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(getProject().getBuildDir(), "markers/" + this.getName());
    }

    @TaskAction
    public void validate() throws IOException {
        // load schema
        JsonSchema jsonSchema;
        FileTree jsonSchemas = Util.getJavaMainSourceResources(
            getProject().findProject(SCHEMA_PROJECT),
            Util.filterByPatterns(getPatternSetFactory().create(), Collections.singletonList(JSON_SCHEMA_PATTERN), null)
        );
        if (jsonSchemas == null || jsonSchemas.getFiles().size() != 1) {
            throw new IllegalStateException(
                String.format(
                    "Could not find the schema file from glob pattern [%s] and project [%s] for JSON spec validation",
                    JSON_SCHEMA_PATTERN,
                    SCHEMA_PROJECT
                )
            );
        }
        File jsonSchemaOnDisk = jsonSchemas.iterator().next();
        getLogger().debug("JSON schema for REST spec: [{}]", jsonSchemaOnDisk.getAbsolutePath());
        SchemaValidatorsConfig config = new SchemaValidatorsConfig();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        jsonSchema = factory.getSchema(mapper.readTree(jsonSchemaOnDisk), config);
        // validate input files
        FileTree specs = getInputDir();
        Map<File, Set<ValidationMessage>> errors = new HashMap<>();
        for (File file : specs.getFiles()) {
            if (ignore.contains(file.getName())) {
                getLogger().info("Ignoring file [{}] due to configuration", file.getName());
                continue;
            }
            // validate all files and hold on to errors for a complete report if there are failures
            getLogger().debug("Validating REST spec [{}]", file.getName());
            Set<ValidationMessage> validationMessages = jsonSchema.validate(mapper.readTree(file));
            for (ValidationMessage validationMessage : validationMessages) {
                errors.computeIfAbsent(file, k -> new HashSet<>()).add(validationMessage);
            }
        }
        if (errors.isEmpty()) {
            Files.write(getSuccessMarker().toPath(), new byte[] {}, StandardOpenOption.CREATE);
        } else {
            // build output and throw exception
            StringBuilder sb = new StringBuilder();
            sb.append("Error validating REST specification.");
            sb.append(System.lineSeparator());
            sb.append(String.format("Schema: %s", jsonSchemaOnDisk));
            sb.append(System.lineSeparator());
            errors.forEach((file, error) -> {
                sb.append(System.lineSeparator());
                sb.append(String.format("File: %s", file.getName()));
                sb.append(System.lineSeparator());
                sb.append("----------------------");
                sb.append(System.lineSeparator());
                error.forEach(message -> {
                    sb.append(message);
                    sb.append(System.lineSeparator());
                });
            });
            throw new JsonSchemaException(sb.toString());
        }
    }
}
