/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.apache.lucene.index.DirectoryReader;

import java.util.Map;
import java.util.Set;

public class IndexEngineTestUtils {

    private IndexEngineTestUtils() {}

    public static Map<DirectoryReader, Set<PrimaryTermAndGeneration>> getOpenReaders(IndexEngine indexEngine) {
        return indexEngine.getOpenReaders();
    }

    public static long getLatestCommittedGeneration(DirectoryReader directoryReader) {
        return IndexEngine.getLatestCommittedGeneration(directoryReader);
    }
}
