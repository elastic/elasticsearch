package org.elasticsearch.snapshots;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public interface Repository {
    Long readLatestIndexId() throws IOException;
    RepositoryData getRepositoryData(Long indexFileGeneration) throws IOException;
    Set<String> getAllIndexIds();
    Date getIndexNTimestamp(Long indexFileGeneration);
    Date getIndexTimestamp(String indexId);
    void deleteIndices(Set<String> leakedIndexIds);

    default void cleanup() throws IOException {
        Long latestIndexId = readLatestIndexId();
        Set<String> referencedIndexIds = getRepositoryData(latestIndexId)
                .getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
        Set<String> allIndexIds = getAllIndexIds();
        Set<String> deletionCandidates = Sets.difference(allIndexIds, referencedIndexIds);
        Date indexNTimestamp = getIndexNTimestamp(latestIndexId);
        Set<String> leakedIndexIds = new HashSet<>();
        for (String candidate : deletionCandidates) {
            Date indexTimestamp = getIndexTimestamp(candidate);
            if (indexTimestamp.before(indexNTimestamp)) {
                leakedIndexIds.add(candidate);
            }
        }
        deleteIndices(leakedIndexIds);
    }
}
