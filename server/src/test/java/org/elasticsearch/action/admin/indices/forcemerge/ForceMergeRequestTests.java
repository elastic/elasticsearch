package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ForceMergeRequestTests extends ESTestCase {

    public void testValidate() {
        final boolean flush = randomBoolean();
        final boolean onlyExpungeDeletes = randomBoolean();
        final int maxNumSegments = randomIntBetween(ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS, 100);

        final ForceMergeRequest request = new ForceMergeRequest();
        request.flush(flush);
        request.onlyExpungeDeletes(onlyExpungeDeletes);
        request.maxNumSegments(maxNumSegments);

        assertThat(request.flush(), equalTo(flush));
        assertThat(request.onlyExpungeDeletes(), equalTo(onlyExpungeDeletes));
        assertThat(request.maxNumSegments(), equalTo(maxNumSegments));

        ActionRequestValidationException validation = request.validate();
        if (onlyExpungeDeletes && maxNumSegments != ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS) {
            assertThat(validation, notNullValue());
            assertThat(validation.validationErrors(), contains("cannot set only_expunge_deletes and max_num_segments at the "
                + "same time, those two parameters are mutually exclusive"));
        } else {
            assertThat(validation, nullValue());
        }
    }
}
