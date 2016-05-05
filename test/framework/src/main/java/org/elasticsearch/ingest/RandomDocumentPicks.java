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

package org.elasticsearch.ingest;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.ingest.core.IngestDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public final class RandomDocumentPicks {

    private RandomDocumentPicks() {

    }

    /**
     * Returns a random field name. Can be a leaf field name or the
     * path to refer to a field name using the dot notation.
     */
    public static String randomFieldName(Random random) {
        int numLevels = RandomInts.randomIntBetween(random, 1, 5);
        String fieldName = "";
        for (int i = 0; i < numLevels; i++) {
            if (i > 0) {
                fieldName += ".";
            }
            fieldName += randomString(random);
        }
        return fieldName;
    }

    /**
     * Returns a random leaf field name.
     */
    public static String randomLeafFieldName(Random random) {
        String fieldName;
        do {
            fieldName = randomString(random);
        } while (fieldName.contains("."));
        return fieldName;
    }

    /**
     * Returns a randomly selected existing field name out of the fields that are contained
     * in the document provided as an argument.
     */
    public static String randomExistingFieldName(Random random, IngestDocument ingestDocument) {
        Map<String, Object> source = new TreeMap<>(ingestDocument.getSourceAndMetadata());
        Map.Entry<String, Object> randomEntry = RandomPicks.randomFrom(random, source.entrySet());
        String key = randomEntry.getKey();
        while (randomEntry.getValue() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) randomEntry.getValue();
            Map<String, Object> treeMap = new TreeMap<>(map);
            randomEntry = RandomPicks.randomFrom(random, treeMap.entrySet());
            key += "." + randomEntry.getKey();
        }
        assert ingestDocument.getFieldValue(key, Object.class) != null;
        return key;
    }

    /**
     * Adds a random non existing field to the provided document and associates it
     * with the provided value. The field will be added at a random position within the document,
     * not necessarily at the top level using a leaf field name.
     */
    public static String addRandomField(Random random, IngestDocument ingestDocument, Object value) {
        String fieldName;
        do {
            fieldName = randomFieldName(random);
        } while (canAddField(fieldName, ingestDocument) == false);
        ingestDocument.setFieldValue(fieldName, value);
        return fieldName;
    }

    /**
     * Checks whether the provided field name can be safely added to the provided document.
     * When the provided field name holds the path using the dot notation, we have to make sure
     * that each node of the tree either doesn't exist or is a map, otherwise new fields cannot be added.
     */
    public static boolean canAddField(String path, IngestDocument ingestDocument) {
        String[] pathElements = path.split("\\.");
        Map<String, Object> innerMap = ingestDocument.getSourceAndMetadata();
        if (pathElements.length > 1) {
            for (int i = 0; i < pathElements.length - 1; i++) {
                Object currentLevel = innerMap.get(pathElements[i]);
                if (currentLevel == null) {
                    return true;
                }
                if (currentLevel instanceof Map == false) {
                    return false;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) currentLevel;
                innerMap = map;
            }
        }
        String leafKey = pathElements[pathElements.length - 1];
        return innerMap.containsKey(leafKey) == false;
    }

    /**
     * Generates a random document and random metadata
     */
    public static IngestDocument randomIngestDocument(Random random) {
        return randomIngestDocument(random, randomSource(random));
    }

    /**
     * Generates a document that holds random metadata and the document provided as a map argument
     */
    public static IngestDocument randomIngestDocument(Random random, Map<String, Object> source) {
        String index = randomString(random);
        String type = randomString(random);
        String id = randomString(random);
        String routing = null;
        if (random.nextBoolean()) {
            routing = randomString(random);
        }
        String parent = null;
        if (random.nextBoolean()) {
            parent = randomString(random);
        }
        String timestamp = null;
        if (random.nextBoolean()) {
            timestamp = randomString(random);
        }
        String ttl = null;
        if (random.nextBoolean()) {
            ttl = randomString(random);
        }
        return new IngestDocument(index, type, id, routing, parent, timestamp, ttl, source);
    }

    public static Map<String, Object> randomSource(Random random) {
        Map<String, Object> document = new HashMap<>();
        addRandomFields(random, document, 0);
        return document;
    }

    /**
     * Generates a random field value, can be a string, a number, a list of an object itself.
     */
    public static Object randomFieldValue(Random random) {
        return randomFieldValue(random, 0);
    }

    private static Object randomFieldValue(Random random, int currentDepth) {
        switch(RandomInts.randomIntBetween(random, 0, 9)) {
            case 0:
                return randomString(random);
            case 1:
                return random.nextInt();
            case 2:
                return random.nextBoolean();
            case 3:
                return random.nextDouble();
            case 4:
                List<String> stringList = new ArrayList<>();
                int numStringItems = RandomInts.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numStringItems; j++) {
                    stringList.add(randomString(random));
                }
                return stringList;
            case 5:
                List<Integer> intList = new ArrayList<>();
                int numIntItems = RandomInts.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numIntItems; j++) {
                    intList.add(random.nextInt());
                }
                return intList;
            case 6:
                List<Boolean> booleanList = new ArrayList<>();
                int numBooleanItems = RandomInts.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numBooleanItems; j++) {
                    booleanList.add(random.nextBoolean());
                }
                return booleanList;
            case 7:
                List<Double> doubleList = new ArrayList<>();
                int numDoubleItems = RandomInts.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numDoubleItems; j++) {
                    doubleList.add(random.nextDouble());
                }
                return doubleList;
            case 8:
                Map<String, Object> newNode = new HashMap<>();
                addRandomFields(random, newNode, ++currentDepth);
                return newNode;
            case 9:
                byte[] byteArray = new byte[RandomInts.randomIntBetween(random, 1, 1024)];
                random.nextBytes(byteArray);
                return byteArray;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static String randomString(Random random) {
        if (random.nextBoolean()) {
            return RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
        }
        return RandomStrings.randomUnicodeOfCodepointLengthBetween(random, 1, 10);
    }

    private static void addRandomFields(Random random, Map<String, Object> parentNode, int currentDepth) {
        if (currentDepth > 5) {
            return;
        }
        int numFields = RandomInts.randomIntBetween(random, 1, 10);
        for (int i = 0; i < numFields; i++) {
            String fieldName = randomLeafFieldName(random);
            Object fieldValue = randomFieldValue(random, currentDepth);
            parentNode.put(fieldName, fieldValue);
        }
    }
}
