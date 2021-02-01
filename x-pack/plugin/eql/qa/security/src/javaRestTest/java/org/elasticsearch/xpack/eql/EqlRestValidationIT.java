package org.elasticsearch.xpack.eql;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.eql.EqlRestValidationTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.eql.SecurityUtils.secureClientSettings;

public class EqlRestValidationIT extends EqlRestValidationTestCase {

    @Override
    protected Settings restClientSettings() {
        return secureClientSettings();
    }

    @Override
    protected String getInexistentIndexErrorMessage() {
        return "\"root_cause\":[{\"type\":\"verification_exception\",\"reason\":\"Found 1 problem\\nline -1:-1: Unknown index [*,-*]\"}],"
            + "\"type\":\"index_not_found_exception\",\"reason\":\"no such index ";
    }

    @Override
    protected void assertErrorMessageWhenAllowNoIndicesIsFalse(String reqParameter) throws IOException {
        assertErrorMessage("inexistent1*", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent1*]\"");
        assertErrorMessage("inexistent1*,inexistent2*", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent1*]\"");
        assertErrorMessage("test_eql,inexistent*", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent*]\"");
        //TODO: revisit the next two tests when https://github.com/elastic/elasticsearch/issues/64190 is closed
        assertErrorMessage("inexistent", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [[inexistent]]\"");
        assertErrorMessage("inexistent1,inexistent2", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [[inexistent1, inexistent2]]\"");
    }

}
