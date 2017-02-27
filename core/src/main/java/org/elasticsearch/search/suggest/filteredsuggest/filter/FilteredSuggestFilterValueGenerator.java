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

package org.elasticsearch.search.suggest.filteredsuggest.filter;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class FilteredSuggestFilterValueGenerator extends FilteredSuggestFilterValues {
    /** Character used to separate several filters */
    public static final String FILTER_NAME_SEPARATOR = String.valueOf('\u001D');

    private final List<FieldInfo> fieldNames;
    private final Iterable<String> values;
    private final boolean indexFilterValue;

    public FilteredSuggestFilterValueGenerator(String fieldname, String fieldIds, Iterable<String> values, boolean indexFilterValue) {
        this.values = values;
        this.indexFilterValue = indexFilterValue;
        if (indexFilterValue) {
            this.fieldNames = allFields(fieldname, fieldIds);
        } else {
            this.fieldNames = null;
        }
    }

    @Override
    public void generate(Document doc, Set<CharSequence> filterValues) {
        if (!indexFilterValue) {
            return;
        }

        if (values == null && fieldNames == null) {
            return;
        }

        if (values != null) {
            Iterator<String> iterator = values.iterator();
            String value;
            while (iterator.hasNext()) {
                value = iterator.next();
                if (isWhitespace(value)) {
                    continue;
                }
                filterValues.add(value);
            }
        } else {
            if (doc.getParent() == null) {
                if (fieldNames.size() == 1) {
                    generateSingleFilterValues(doc, fieldNames.get(0), 0, filterValues);
                } else {
                    Map<Integer, List<FilterIndexableField>> filterFields = new LinkedHashMap<>();
                    Set<String> longestfilterValues = new HashSet<>();
                    generateMultiFilterValues(doc, fieldNames, 0, 0, filterFields, longestfilterValues);
                    if (longestfilterValues.size() > 0) {
                        filterValues.addAll(listAllCombinations(longestfilterValues));
                    }
                }
            } else {
                String name;
                Stack<Document> docToProcess = new Stack<>();
                int index = fieldNames.size() - 1;
                Document parent = doc;
                Document current = null;
                while (true) {
                    name = fieldNames.get(index).getName();
                    if (name.contains(".")) {
                        name = name.substring(0, name.lastIndexOf("."));
                    } else {
                        name = "";
                    }
                    if (parent.getPath().equals(name) && parent != current) {
                        docToProcess.push(parent);
                        current = parent;
                        index--;
                    } else {
                        if (parent.getPath().split("\\.").length < name.split("\\.").length) {
                            index--;
                        } else {
                            parent = parent.getParent();
                        }
                    }

                    if (parent == null || index < 0) {
                        break;
                    }
                }
                name = null;
                if (docToProcess.size() > 0) {
                    parent = docToProcess.peek();
                } else if (parent == null) {
                    return;
                } else {
                    docToProcess.push(parent);
                }

                Integer pathIndex = -1;
                if (!parent.getPath().equals("")) {
                    pathIndex = (parent.getPath().split("\\.").length - 1);
                }

                if (fieldNames.size() == 1) {
                    generateSingleFilterValues(docToProcess, fieldNames.get(0), pathIndex, filterValues);
                } else {
                    Map<Integer, List<FilterIndexableField>> filterFields = new LinkedHashMap<>();
                    Set<String> longestfilterValues = new HashSet<>();
                    generateMultiFilterValues(docToProcess, fieldNames, 0, pathIndex, filterFields, longestfilterValues);
                    if (longestfilterValues.size() > 0) {
                        filterValues.addAll(listAllCombinations(longestfilterValues));
                    }
                }
            }
        }
    }

    private static List<FieldInfo> allFields(String fieldName, String fieldIds) {
        if (fieldName == null) {
            return null;
        }

        List<FieldInfo> names = new ArrayList<>();
        if (fieldName.contains(FILTER_NAME_SEPARATOR)) {
            String[] fieldNames = fieldName.split(FILTER_NAME_SEPARATOR);
            String[] filterIds = fieldIds.split(FILTER_NAME_SEPARATOR);
            for (int i = 0; i < fieldNames.length; i++) {
                names.add(new FieldInfo(filterIds[i], fieldNames[i]));
            }
            return names;
        }

        names.add(new FieldInfo(fieldIds, fieldName));

        return Collections.unmodifiableList(names);
    }

    /**
     * Suggest field is defined at the root document.
     * 
     * @param currentDoc
     *            root document
     * @param fieldInfo
     *            filter value to read from
     * @param pathIndex
     *            current index of the filter field path
     * @param filterValues
     *            read values
     */
    private void generateSingleFilterValues(Document currentDoc, FieldInfo fieldInfo, Integer pathIndex, Set<CharSequence> filterValues) {
        if (pathIndex.equals((fieldInfo.getPathLength() - 1))) {
            IndexableField[] fields = currentDoc.getFields(fieldInfo.getTreePath(pathIndex));
            if (fields.length != 0) {
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].fieldType().docValuesType() == DocValuesType.NONE) {
                        addFieldValue(filterValues, fieldInfo.getFieldId(), fields[i]);
                        break;
                    }
                }
            }

            return;
        }
        List<Document> childDocs = currentDoc.getChildren(fieldInfo.getTreePath(pathIndex));
        if (childDocs != null) {
            pathIndex++;
            for (Document child : childDocs) {
                generateSingleFilterValues(child, fieldInfo, pathIndex, filterValues);
            }
        }
    }

    /**
     * This is the case when suggest field is defined at the inner level but not at the root level. Since it is defined
     * at inner level we only have to process its parent instance and all its child instances.
     * 
     * @param docs
     *            Instance reference of parents.
     * @param fieldInfo
     *            filter value to read from
     * @param pathIndex
     *            current index of the filter field path
     * @param filterValues
     *            read values
     */
    private void generateSingleFilterValues(Stack<Document> docs, FieldInfo fieldInfo, Integer pathIndex, Set<CharSequence> filterValues) {
        Document lastDoc = null;
        boolean found = false;
        while (!docs.isEmpty()) {
            lastDoc = docs.pop();
            pathIndex++;

            IndexableField[] fields = lastDoc.getFields(fieldInfo.getName());
            if (fields.length != 0) {
                found = true;
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].fieldType().docValuesType() == DocValuesType.NONE) {
                        addFieldValue(filterValues, fieldInfo.getFieldId(), fields[i]);
                        break;
                    }
                }
            }

            if (found) {
                return;
            }
        }

        generateSingleFilterValues(lastDoc, fieldInfo, pathIndex, filterValues);
    }

    private Integer generateMultiFilterValues(Document currentDoc, List<FieldInfo> fieldInfos, Integer filterFieldIndex, Integer pathIndex,
            Map<Integer, List<FilterIndexableField>> filterFields, Set<String> filterValues) {
        if (filterFieldIndex == fieldInfos.size()) {
            return filterFieldIndex;
        }

        FieldInfo fieldInfo = fieldInfos.get(filterFieldIndex);

        if (pathIndex.equals((fieldInfo.getPathLength() - 1))) {
            IndexableField[] fields = currentDoc.getFields(fieldInfo.getTreePath(pathIndex));
            if (fields.length != 0) {
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].fieldType().docValuesType() == DocValuesType.NONE) {
                        List<FilterIndexableField> fieldRefs = filterFields.get(pathIndex);
                        if (fieldRefs == null) {
                            fieldRefs = new ArrayList<>(4);
                            filterFields.put(pathIndex, fieldRefs);
                        }
                        fieldRefs.add(new FilterIndexableField(fieldInfo.getFieldId(), fields[i]));
                        break;
                    }
                }
            }

            if (filterFieldIndex == (fieldInfos.size() - 1)) {
                return filterFieldIndex;
            } else {
                filterFieldIndex++;
                return generateMultiFilterValues(currentDoc, fieldInfos, filterFieldIndex, pathIndex, filterFields, filterValues);
            }
        }

        List<Document> childDocs = currentDoc.getChildren(fieldInfo.getTreePath(pathIndex));
        if (childDocs != null) {
            pathIndex++;
            Integer currentFieldIndex;
            for (Document child : childDocs) {
                currentFieldIndex = generateMultiFilterValues(child, fieldInfos, filterFieldIndex, pathIndex, filterFields, filterValues);
                if (currentFieldIndex == (fieldInfos.size() - 1) && filterFields.size() > 0) {
                    buildFilterValue(filterFields, filterValues);
                }

                filterFields.remove(pathIndex);
            }
            --filterFieldIndex;
        } else {
            return fieldInfos.size() - 1;
        }

        return filterFieldIndex;
    }

    private void generateMultiFilterValues(Stack<Document> docs, List<FieldInfo> fieldInfos, int filterFieldIndex, int pathIndex,
            Map<Integer, List<FilterIndexableField>> filterFields, Set<String> filterValues) {
        Document lastDoc = null;
        boolean runAtSameLevel = false;
        while (!docs.isEmpty()) {
            if (!runAtSameLevel) {
                lastDoc = docs.pop();
                pathIndex++;
                runAtSameLevel = false;
            }

            FieldInfo fieldInfo = fieldInfos.get(filterFieldIndex);

            IndexableField[] fields = lastDoc.getFields(fieldInfo.getName());
            if (fields.length != 0) {
                filterFieldIndex++;
                runAtSameLevel = true;
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].fieldType().docValuesType() == DocValuesType.NONE) {
                        List<FilterIndexableField> fieldRefs = filterFields.get(pathIndex);
                        if (fieldRefs == null) {
                            fieldRefs = new ArrayList<>(4);
                            filterFields.put(pathIndex, fieldRefs);
                        }
                        fieldRefs.add(new FilterIndexableField(fieldInfo.getFieldId(), fields[i]));
                        break;
                    }
                }
            } else {
                runAtSameLevel = false;
            }
        }

        if (filterFieldIndex == fieldInfos.size() && filterFields.size() > 0) {
            buildFilterValue(filterFields, filterValues);
        } else {
            generateMultiFilterValues(lastDoc, fieldInfos, filterFieldIndex, pathIndex, filterFields, filterValues);
        }
    }

    private static boolean isWhitespace(String value) {
        return value == null || value.length() == 0 || Character.isWhitespace(value.charAt(0));
    }

    private static void buildFilterValue(Map<Integer, List<FilterIndexableField>> filterFields, Set<String> filterValues) {
        StringBuilder builder = new StringBuilder(calculateSpace(filterFields));
        for (List<FilterIndexableField> fFields : filterFields.values()) {
            for (FilterIndexableField fField : fFields) {
                if (isWhitespace(fField.field.stringValue())) {
                    continue;
                }

                builder.append(fField.fieldId);
                builder.append(fField.field.stringValue());
                builder.append(FILTER_NAME_SEPARATOR);
            }
        }

        if (builder.length() == 0) {
            return;
        }

        filterValues.add(builder.substring(0, builder.length() - 1));
    }

    private static int calculateSpace(Map<Integer, List<FilterIndexableField>> filterFields) {
        int length = 0;
        for (List<FilterIndexableField> fFields : filterFields.values()) {
            for (FilterIndexableField fField : fFields) {
                length = length + fField.getValueLength() + FILTER_NAME_SEPARATOR.length();
            }
        }

        return length;
    }

    private static void addFieldValue(Set<CharSequence> filterValues, String fieldId, IndexableField field) {
        if (isWhitespace(field.stringValue())) {
            return;
        }

        filterValues.add(fieldId.concat(field.stringValue()));
    }

    private static Set<String> listAllCombinations(Set<String> fieldValuesList) {
        Set<String> combinations = new HashSet<>();

        for (String valuePath : fieldValuesList) {
            combinations.addAll(listAllCombinations(valuePath));
        }

        return combinations;
    }

    private static Set<String> listAllCombinations(String valuePath) {
        Set<String> combinations = new HashSet<>();
        StringBuilder br = new StringBuilder();

        String[] valuePathArray = valuePath.split(FILTER_NAME_SEPARATOR);
        if (valuePathArray.length == 1) {
            combinations.add(valuePathArray[0]);
            return combinations;
        }

        ICombinatoricsVector<String> initialVector = Factory.createVector(valuePathArray);
        int count = valuePathArray.length;
        while (count > 0) {
            Generator<String> gen = Factory.createSimpleCombinationGenerator(initialVector, count);
            for (ICombinatoricsVector<String> combination : gen) {
                br.setLength(0);
                for (String str : combination) {
                    br.append(str);
                }
                combinations.add(br.toString());
            }
            count--;
        }

        return combinations;
    }

    private static class FilterIndexableField {
        private String fieldId;
        private IndexableField field;

        FilterIndexableField(String fieldId, IndexableField field) {
            this.fieldId = fieldId;
            this.field = field;
        }

        public int getValueLength() {
            return fieldId.length() + field.stringValue().length();
        }
    }

    private static class FieldInfo {

        String fieldId;
        String name;
        String[] path;
        Map<Integer, String> treePaths = new HashMap<>();

        FieldInfo(String fieldId, String name) {
            this.fieldId = fieldId;
            this.name = name;
            path = this.name.split("\\.");
        }

        public Integer getPathLength() {
            return path.length;
        }

        public String getFieldId() {
            return fieldId;
        }

        public String getName() {
            return name;
        }

        public String getTreePath(int index) {
            String treePath = treePaths.get(index);
            if (treePath == null) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i <= index; i++) {
                    builder.append(path[i]);
                    if (i < index) {
                        builder.append(".");
                    }
                }

                treePath = builder.toString();
                treePaths.put(index, treePath);
            }

            return treePath;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
