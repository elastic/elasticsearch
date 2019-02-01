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
package org.elasticsearch.client.indices.rollover;

import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Request class to swap index under an alias upon satisfying conditions
 */
public class RolloverRequest extends TimedRequest implements Validatable, ToXContentObject {

    private String alias;
    private String newIndexName;
    private boolean dryRun;
    private Map<String, Condition<?>> conditions = new HashMap<>(2);
    //the index name "_na_" is never read back, what matters are settings, mappings and aliases
    private CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

    public RolloverRequest(String alias, String newIndexName) {
        this.alias = alias;
        this.newIndexName = newIndexName;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (alias == null) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("index alias is missing");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Sets the alias to rollover to another index
     */
    public RolloverRequest alias(String alias) {
        this.alias = alias;
        return this;
    }
    /**
     * Returns the alias of the rollover operation
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Sets the new index name for the rollover
     */
    public RolloverRequest newIndexName(String newIndexName) {
        this.newIndexName = newIndexName;
        return this;
    }
    /**
     * Returns the new index name for the rollover
     */
    public String getNewIndexName() {
        return newIndexName;
    }


    /**
     * Sets if the rollover should not be executed when conditions are met
     */
    public RolloverRequest dryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }
    /**
     * Returns if the rollover should not be executed when conditions are met
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Adds condition to check if the index is at least <code>age</code> old
     */
    public RolloverRequest addMaxIndexAgeCondition(TimeValue age) {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(age);
        if (this.conditions.containsKey(maxAgeCondition.name())) {
            throw new IllegalArgumentException(maxAgeCondition.name() + " condition is already set");
        }
        this.conditions.put(maxAgeCondition.name(), maxAgeCondition);
        return this;
    }

    /**
     * Adds condition to check if the index has at least <code>numDocs</code>
     */
    public RolloverRequest addMaxIndexDocsCondition(long numDocs) {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(numDocs);
        if (this.conditions.containsKey(maxDocsCondition.name())) {
            throw new IllegalArgumentException(maxDocsCondition.name() + " condition is already set");
        }
        this.conditions.put(maxDocsCondition.name(), maxDocsCondition);
        return this;
    }
    /**
     * Adds a size-based condition to check if the index size is at least <code>size</code>.
     */
    public RolloverRequest addMaxIndexSizeCondition(ByteSizeValue size) {
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(size);
        if (this.conditions.containsKey(maxSizeCondition.name())) {
            throw new IllegalArgumentException(maxSizeCondition + " condition is already set");
        }
        this.conditions.put(maxSizeCondition.name(), maxSizeCondition);
        return this;
    }
    /**
     * Returns all set conditions
     */
    public Map<String, Condition<?>> getConditions() {
        return conditions;
    }

    /**
     * Returns the inner {@link CreateIndexRequest}. Allows to configure mappings, settings and aliases for the new index.
     */
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        createIndexRequest.innerToXContent(builder, params);

        builder.startObject("conditions");
        for (Condition<?> condition : conditions.values()) {
            condition.toXContent(builder, params);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

}
