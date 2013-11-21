package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.search.NoopCollector;

import java.io.IOException;

class BitSetCollector extends NoopCollector {

    final FixedBitSet result;
    int docBase;

    BitSetCollector(int topLevelMaxDoc) {
        this.result = new FixedBitSet(topLevelMaxDoc);
    }

    @Override
    public void collect(int doc) throws IOException {
        result.set(docBase + doc);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        docBase = context.docBase;
    }

    FixedBitSet getResult() {
        return result;
    }

}
