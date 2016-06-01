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

package org.elasticsearch.action.quality;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Collection;

/**
 * This class defines a qa task including query intent and query spec.
 *
 * Each QA run is based on a set of queries to send to the index and multiple QA specifications that define how to translate the query
 * intents into elastic search queries. In addition it contains the quality metrics to compute.
 * */

public class QualityTask {
    /** Collection of query intents to check against including expected document ids.*/
    private Collection<Intent> intents;
    /** Collection of query specifications, that is e.g. search request templates to use for query translation. */
    private Collection<Specification> specifications;
    /** Definition of n in precision at n */
    private QualityContext config;
    
    /** Returns the precision at n configuration (containing level of n to consider).*/
    public QualityContext getQualityContext() {
        return config;
    }

    /** Sets the precision at n configuration (containing level of n to consider).*/
    public void setConfig(QualityContext config) {
        this.config = config;
    }

    /** Returns a list of search intents to evaluate. */
    public Collection<Intent> getIntents() {
        return intents;
    }

    /** Set a list of search intents to evaluate. */
    public void setIntents(Collection<Intent> intents) {
        this.intents = intents;
    }

    /** Returns a list of intent to query translation specifications to evaluate. */
    public Collection<Specification> getSpecifications() {
        return specifications;
    }

    /** Set the list of intent to query translation specifications to evaluate. */
    public void setSpecifications(Collection<Specification> specifications) {
        this.specifications = specifications;
    }

    @Override
    public String toString() {
        ToStringHelper help = MoreObjects.toStringHelper(this).add("Intent", intents);
        help.add("Specifications", specifications);
        help.add("Precision configuration", config);
        return help.toString();
    }
}
