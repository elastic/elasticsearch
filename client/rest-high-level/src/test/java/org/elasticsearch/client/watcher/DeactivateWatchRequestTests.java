package org.elasticsearch.client.watcher;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.hasItem;

public class DeactivateWatchRequestTests extends ESTestCase {

    public void testNullId() {
        DeactivateWatchRequest request = new DeactivateWatchRequest(null);
        Optional<ValidationException> actual = request.validate();
        assertTrue(actual.isPresent());
        assertThat(actual.get().validationErrors(), hasItem("watch id is missing"));
    }

    public void testInvalidId() {
        DeactivateWatchRequest request = new DeactivateWatchRequest("Watch Id with spaces");
        Optional<ValidationException> actual = request.validate();
        assertTrue(actual.isPresent());
        assertThat(actual.get().validationErrors(), hasItem("watch id contains whitespace"));
    }

}
