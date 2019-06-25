package org.elasticsearch.snapshots;

import org.elasticsearch.repositories.RepositoryData;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

public interface Repository {
    Long readLatestIndexId() throws IOException;
    RepositoryData getRepositoryData(Long indexFileGeneration) throws IOException;
    Set<String> getAllIndexIds();
    Date getIndexNTimestamp(Long indexFileGeneration);
    Date getIndexTimestamp(String indexId);
    void deleteIndices(Set<String> leakedIndexIds);
    void cleanup() throws IOException;
}
