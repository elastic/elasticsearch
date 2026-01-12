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

package co.elastic.elasticsearch.stateless.autoscaling;

import static org.elasticsearch.test.ESTestCase.randomFloat;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

public class DesiredClusterTopologyTestUtils {

    public static DesiredClusterTopology randomDesiredClusterTopology() {
        return new DesiredClusterTopology(randomTierTopology(), randomTierTopology());
    }

    public static DesiredClusterTopology.TierTopology randomTierTopology() {
        return new DesiredClusterTopology.TierTopology(
            randomIntBetween(1, 10),
            randomLongBetween(1, 8_589_934_592L) + "b",
            randomFloat(),
            randomFloat(),
            randomFloat()
        );
    }
}
