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

package org.elasticsearch.join.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class Joiner {

    public static Joiner getJoiner(QueryShardContext context) {
        return getJoiner(context::isFieldMapped, context::getFieldType);
    }

    public static Joiner getJoiner(AggregationContext context) {
        return getJoiner(context::isFieldMapped, context::getFieldType);
    }

    public static Joiner getJoiner(Predicate<String> isMapped, Function<String, MappedFieldType> getFieldType) {
        if (isMapped.test(MetaJoinFieldMapper.NAME) == false) {
            return null;
        }
        MetaJoinFieldMapper.MetaJoinFieldType ft
            = (MetaJoinFieldMapper.MetaJoinFieldType) getFieldType.apply(MetaJoinFieldMapper.NAME);
        String joinField = ft.getJoinField();
        if (isMapped.test(joinField) == false) {
            return null;
        }
        ParentJoinFieldMapper.JoinFieldType jft =
            (ParentJoinFieldMapper.JoinFieldType) getFieldType.apply(joinField);
        return jft.getJoiner();
    }

    private final Map<String, Set<String>> parentsToChildren = new HashMap<>();
    private final Map<String, String> childrenToParents = new HashMap<>();

    private final String joinField;

    public Joiner(String joinField, List<Relations> relations) {
        this.joinField = joinField;
        for (Relations r : relations) {
            for (String child : r.children) {
                parentsToChildren.put(r.parent, r.children);
                if (childrenToParents.containsKey(child)) {
                    throw new IllegalArgumentException("[" + child + "] cannot have multiple parents");
                }
                childrenToParents.put(child, r.parent);
            }
        }
    }

    public String getJoinField() {
        return joinField;
    }

    public Query filter(String relationType) {
        return new TermQuery(new Term(joinField, relationType));
    }

    public Query parentFilter(String childType) {
        return new TermQuery(new Term(joinField, childrenToParents.get(childType)));
    }

    public Query getChildrenFilter(String parentType) {
        assert parentTypeExists(parentType);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String child : parentsToChildren.get(parentType)) {
            builder.add(filter(child), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    public boolean childTypeExists(String type) {
        return childrenToParents.containsKey(type);
    }

    public boolean parentTypeExists(String type) {
        return parentsToChildren.containsKey(type);
    }

    public boolean knownRelation(String type) {
        return childTypeExists(type) || parentTypeExists(type);
    }

    public String parentJoinField(String childType) {
        return joinField + "#" + childrenToParents.get(childType);
    }

    public String childJoinField(String parentType) {
        return joinField + "#" + parentType;
    }

    boolean canMerge(Joiner other, Consumer<String> conflicts) {
        boolean conflicted = false;
        for (String parent : parentsToChildren.keySet()) {
            if (other.parentsToChildren.containsKey(parent) == false) {
                conflicts.accept("Cannot remove parent [" + parent + "]");
                conflicted = true;
            }
        }
        for (String child : childrenToParents.keySet()) {
            if (other.childrenToParents.containsKey(child) == false) {
                conflicts.accept("Cannot remove child [" + child + "]");
                conflicted = true;
            }
        }
        for (String newParent : other.parentsToChildren.keySet()) {
            if (childrenToParents.containsKey(newParent) && parentsToChildren.containsKey(newParent) == false) {
                conflicts.accept("Cannot create parent [" + newParent + "] from an existing child");
                conflicted = true;
            }
        }
        for (String newChild : other.childrenToParents.keySet()) {
            if (this.childrenToParents.containsKey(newChild)
                && Objects.equals(other.childrenToParents.get(newChild), this.childrenToParents.get(newChild)) == false) {
                conflicts.accept("Cannot change parent of [" + newChild + "]");
                conflicted = true;
            }
            if (this.parentsToChildren.containsKey(newChild)
                && Objects.equals(this.childrenToParents.get(newChild), other.childrenToParents.get(newChild)) == false) {
                conflicts.accept("Cannot create child [" + newChild + "] from an existing root");
                conflicted = true;
            }
        }
        return conflicted == false;
    }

}
