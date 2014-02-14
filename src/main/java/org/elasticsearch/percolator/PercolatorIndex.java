package org.elasticsearch.percolator;

import org.elasticsearch.index.mapper.ParsedDocument;


interface PercolatorIndex {

    void prepare(PercolateContext context, ParsedDocument document);

    void clean();

}
