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

package co.elastic.elasticsearch.stateless.recovery;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.snapshots.IndexMetadataRestoreTransformer;

/**
 * This is a temporary class meant to remediate missing allocator settings in indices that have been snapshot. Affected indices
 * will be remediated via {@link RemedialAllocationSettingService}, but any snapshots taken before this remediation executes will
 * persist the incorrect settings in the snapshot data. If these snapshots are ever restored, then the remediation will be undone
 * and the indices restored may be subject to allocation issues. This transformer only needs to exist for as long as those snapshots
 * are around. Once they age out, this can be removed.
 */
public class StatelessRestoreTransformer implements IndexMetadataRestoreTransformer {
    @Override
    public IndexMetadata updateIndexMetadata(IndexMetadata original) {
        if (ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.exists(original.getSettings()) == false) {
            // Remediate missing index setting
            return RemedialAllocationSettingService.addStatelessExistingShardsAllocatorSetting(original);
        }
        return original;
    }
}
