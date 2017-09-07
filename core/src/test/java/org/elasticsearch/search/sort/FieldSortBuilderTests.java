package org.elasticsearch.search.sort;

import static org.hamcrest.Matchers.equalToIgnoringWhiteSpace;

import org.elasticsearch.test.ESTestCase;

public class FieldSortBuilderTests extends ESTestCase {

    public void testToString() throws Exception {
        FieldSortBuilder fsb = SortBuilders.fieldSort("myfield");
        fsb.order(SortOrder.ASC);

        String fsbPrint = fsb.toString();

        assertThat(fsbPrint, equalToIgnoringWhiteSpace("{ \"myfield\" : { \"order\" : \"asc\" } }"));
    }

}
