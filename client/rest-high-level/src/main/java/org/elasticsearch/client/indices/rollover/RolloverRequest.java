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

import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Request class to swap index under an alias upon satisfying conditions
 */
public class RolloverRequest extends TimedRequest implements Validatable, ToXContentObject {

    static final String AGE_CONDITION = "max_age";
    static final String DOCS_CONDITION = "max_docs";
    static final String SIZE_CONDITION = "max_size";

    private String alias;
    private String newIndexName;
    private boolean dryRun;
    private Map<String, Object> conditions = new HashMap<>(2);
    //the index name "_na_" is never read back, what matters are settings, mappings and aliases
    private CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

    public RolloverRequest(String alias, String newIndexName) {
        this.alias = alias;
        this.newIndexName = newIndexName;
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
        if (conditions.containsKey(AGE_CONDITION)) {
            throw new IllegalArgumentException(AGE_CONDITION + " condition is already set");
        }
        this.conditions.put(AGE_CONDITION, age);
        return this;
    }

    /**
     * Adds condition to check if the index has at least <code>numDocs</code>
     */
    public RolloverRequest addMaxIndexDocsCondition(long numDocs) {
        if (conditions.containsKey(DOCS_CONDITION)) {
            throw new IllegalArgumentException(DOCS_CONDITION + " condition is already set");
        }
        this.conditions.put(DOCS_CONDITION, numDocs);
        return this;
    }
    /**
     * Adds a size-based condition to check if the index size is at least <code>size</code>.
     */
    public RolloverRequest addMaxIndexSizeCondition(ByteSizeValue size) {
        if (conditions.containsKey(SIZE_CONDITION)) {
            throw new IllegalArgumentException(SIZE_CONDITION + " condition is already set");
        }
        this.conditions.put(SIZE_CONDITION, size);
        return this;
    }
    /**
     * Returns all set conditions
     */
    public Map<String, Object> getConditions() {
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
        for (Map.Entry<String, Object> entry : conditions.entrySet())
        {
            String name = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof TimeValue) {
                builder.field(name, ((TimeValue) value).getStringRep());
            } else if (value instanceof ByteSizeValue) {
                builder.field(name, ((ByteSizeValue) value).getStringRep());
            } else { //instance of Long
                builder.field(name, (long) value);
            }
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

}
