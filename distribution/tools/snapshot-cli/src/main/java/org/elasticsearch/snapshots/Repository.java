package org.elasticsearch.snapshots;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.repositories.RepositoryData;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

public interface Repository {
    Tuple<Long, Date> getLatestIndexIdAndTimestamp() throws IOException;
    RepositoryData getRepositoryData(long indexFileGeneration) throws IOException;
    Set<String> getAllIndexDirectoryNames();
    Date getIndexTimestamp(String indexId);
    void deleteIndices(Set<String> orphanedIndexIds);
    void cleanup() throws IOException;
}
