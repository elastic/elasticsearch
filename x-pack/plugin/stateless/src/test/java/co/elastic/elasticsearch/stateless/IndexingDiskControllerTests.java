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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingDiskControllerTests extends ESTestCase {

    public void testReservedBytesDoesNotExceedLuceneIndexingBuffer() throws Exception {
        final var indicesService = mock(IndicesService.class);
        when(indicesService.getTotalIndexingBufferBytes()).thenReturn(ByteSizeValue.ofKb(10L));

        try (var nodeEnvironment = newNodeEnvironment()) {
            var exception = expectThrows(
                IllegalStateException.class,
                () -> new IndexingDiskController(
                    nodeEnvironment,
                    Settings.builder().put(IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING.getKey(), "1kb").build(),
                    mock(ThreadPool.class),
                    indicesService,
                    mock(StatelessCommitService.class)
                )
            );
            assertThat(
                exception.getMessage(),
                equalTo("Reserved disk space [1kb (1024 bytes)] must be larger than Lucene indexing buffer [10kb (10240 bytes)]")
            );
        }
    }
}
