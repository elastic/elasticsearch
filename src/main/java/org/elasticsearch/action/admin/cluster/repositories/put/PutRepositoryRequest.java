/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;

/**
 * Register repository request.
 * <p/>
 * Registers a repository with given name, type and settings. If the repository with the same name already
 * exists in the cluster, the new repository will replace the existing repository.
 */
public class PutRepositoryRequest extends AcknowledgedRequest<PutRepositoryRequest> {

    private String name;

    private String type;

    private boolean verify = true;

    private Settings settings = EMPTY_SETTINGS;

    PutRepositoryRequest() {
    }

    /**
     * Constructs a new put repository request with the provided name.
     */
    public PutRepositoryRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the repository.
     *
     * @param name repository name
     */
    public PutRepositoryRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the repository.
     *
     * @return repository name
     */
    public String name() {
        return this.name;
    }

    /**
     * The type of the repository
     * <p/>
     * <ul>
     * <li>"fs" - shared filesystem repository</li>
     * </ul>
     *
     * @param type repository type
     * @return this request
     */
    public PutRepositoryRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Returns repository type
     *
     * @return repository type
     */
    public String type() {
        return this.type;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source repository settings in json, yaml or properties format
     * @return this request
     */
    public PutRepositoryRequest settings(String source) {
        this.settings = ImmutableSettings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Sets whether or not the repository should be verified after creation
     */
    public PutRepositoryRequest verify(boolean verify) {
        this.verify = verify;
        return this;
    }

    /**
     * Returns true if repository should be verified after creation
     */
    public boolean verify() {
        return this.verify;
    }

    /**
     * Parses repository definition.
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(XContentBuilder repositoryDefinition) {
        return source(repositoryDefinition.bytes());
    }

    /**
     * Parses repository definition.
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(Map repositoryDefinition) {
        Map<String, Object> source = repositoryDefinition;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("type")) {
                type(entry.getValue().toString());
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new ElasticsearchIllegalArgumentException("Malformed settings section, should include an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            }
        }
        return this;
    }

    /**
     * Parses repository definition.
     * JSON, Smile and YAML formats are supported
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(String repositoryDefinition) {
        try {
            return source(XContentFactory.xContent(repositoryDefinition).createParser(repositoryDefinition).mapOrderedAndClose());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse repository source [" + repositoryDefinition + "]", e);
        }
    }

    /**
     * Parses repository definition.
     * JSON, Smile and YAML formats are supported
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(byte[] repositoryDefinition) {
        return source(repositoryDefinition, 0, repositoryDefinition.length);
    }

    /**
     * Parses repository definition.
     * JSON, Smile and YAML formats are supported
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(byte[] repositoryDefinition, int offset, int length) {
        try {
            return source(XContentFactory.xContent(repositoryDefinition, offset, length).createParser(repositoryDefinition, offset, length).mapOrderedAndClose());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse repository source", e);
        }
    }

    /**
     * Parses repository definition.
     * JSON, Smile and YAML formats are supported
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(BytesReference repositoryDefinition) {
        try {
            return source(XContentFactory.xContent(repositoryDefinition).createParser(repositoryDefinition).mapOrderedAndClose());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse template source", e);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        type = in.readString();
        settings = readSettingsFromStream(in);
        readTimeout(in);
        verify = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(type);
        writeSettingsToStream(settings, out);
        writeTimeout(out);
        out.writeBoolean(verify);
    }
}
