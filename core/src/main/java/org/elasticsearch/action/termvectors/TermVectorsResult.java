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

package org.elasticsearch.action.termvectors;

import org.apache.lucene.index.Fields;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.util.EnumSet;
import java.util.Set;

public class TermVectorsResult {

    private String concreteIndex;
    private String type;
    private String id;

    private Long docVersion;
    private boolean exists = false;
    private boolean artificial = false;
    private Fields termVectorsByField;
    private Set<String> selectedFields;
    private EnumSet<TermVectorsRequest.Flag> flags;
    private Fields topLevelFields;
    private AggregatedDfs dfs;
    private TermVectorsFilter termVectorsFilter;

    public TermVectorsResult(String concreteIndex, String type, String id) {
        this.concreteIndex = concreteIndex;
        this.type = type;
        this.id = id;
    }

    public void setDocVersion(Long docVersion) {
        this.docVersion = docVersion;
    }

    public void setArtificial(boolean artificial) {
        this.artificial = artificial;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<TermVectorsRequest.Flag> flags,
                          Fields topLevelFields, AggregatedDfs dfs, TermVectorsFilter termVectorsFilter) {
        this.termVectorsByField = termVectorsByField;
        this.selectedFields = selectedFields;
        this.flags = flags;
        this.topLevelFields = topLevelFields;
        this.dfs = dfs;
        this.termVectorsFilter = termVectorsFilter;
    }

    public Long getDocVersion() {
        return docVersion;
    }

    public boolean getArtificial() {
        return artificial;
    }

    public boolean getExists() {
        return exists;
    }

    public Fields getTermVectorsByField() {
        return termVectorsByField;
    }

    public Fields getTopLevelFields() {
        return topLevelFields;
    }

    public Set<String> getSelectedFields() {
        return selectedFields;
    }

    public AggregatedDfs getDfs() {
        return dfs;
    }

    public TermVectorsFilter getTermVectorsFilter() {
        return termVectorsFilter;
    }

    public EnumSet<TermVectorsRequest.Flag> getFlags() {
        return flags;
    }

    public String getConcreteIndex() {
        return concreteIndex;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }
}
