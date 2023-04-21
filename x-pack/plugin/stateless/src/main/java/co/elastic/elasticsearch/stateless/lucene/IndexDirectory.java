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

package co.elastic.elasticsearch.stateless.lucene;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

public class IndexDirectory extends FilterDirectory {

    private final Set<String> deletedFiles = ConcurrentCollections.newConcurrentSet();

    private final SearchDirectory searchDirectory;

    public IndexDirectory(Directory in, SharedBlobCacheService<FileCacheKey> cacheService, ShardId shardId) {
        super(in);
        this.searchDirectory = new SearchDirectory(cacheService, shardId);
    }

    @Override
    public String[] listAll() throws IOException {
        final String[] remote = searchDirectory.listAll();
        final String[] local = super.listAll();
        return Stream.concat(Arrays.stream(remote).filter(name -> deletedFiles.contains(name) == false), Arrays.stream(local))
            .distinct()
            .sorted(String::compareTo)
            .toArray(String[]::new);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (deletedFiles.contains(name)) {
            throw new NoSuchFileException(name);
        }
        if (searchDirectory.containsFile(name)) {
            return searchDirectory.fileLength(name);
        }
        return super.fileLength(name);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (searchDirectory.containsFile(name)) {
            return searchDirectory.openInput(name, context);
        }
        return super.openInput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (searchDirectory.containsFile(name)) {
            deletedFiles.add(name);
            try {
                super.deleteFile(name);
            } catch (IOException e) {
                // ignored
            }
            return;
        }
        super.deleteFile(name);
    }

    public void sync(Collection<String> names) {
        // noop, data on local drive need not be safely persisted
    }

    @Override
    public void syncMetaData() {
        // noop, data on local drive need not be safely persisted
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(searchDirectory, super::close);
    }

    public SearchDirectory getSearchDirectory() {
        return searchDirectory;
    }
}
