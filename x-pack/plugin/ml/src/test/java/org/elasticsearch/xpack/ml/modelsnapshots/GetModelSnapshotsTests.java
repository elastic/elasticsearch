/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.modelsnapshots;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.TransportGetModelSnapshotsAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;

import java.util.Arrays;
import java.util.Date;

public class GetModelSnapshotsTests extends ESTestCase {

    public void testModelSnapshots_GivenNegativeFrom() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GetModelSnapshotsAction.Request("foo", null).setPageParams(new PageParams(-5, 10)));
        assertEquals("Parameter [from] cannot be < 0", e.getMessage());
    }

    public void testModelSnapshots_GivenNegativeSize() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GetModelSnapshotsAction.Request("foo", null).setPageParams(new PageParams(10, -5)));
        assertEquals("Parameter [size] cannot be < 0", e.getMessage());
    }

    public void testModelSnapshots_clearQuantiles() {
        ModelSnapshot m1 = new ModelSnapshot.Builder("jobId").setQuantiles(
                new Quantiles("jobId", new Date(), "quantileState")).build();
        ModelSnapshot m2 = new ModelSnapshot.Builder("jobId").build();

        QueryPage<ModelSnapshot> page = new QueryPage<>(Arrays.asList(m1, m2), 2, new ParseField("field"));
        page = TransportGetModelSnapshotsAction.clearQuantiles(page);
        assertEquals(2, page.results().size());
        for (ModelSnapshot modelSnapshot : page.results()) {
            assertNull(modelSnapshot.getQuantiles());
        }
    }
}
