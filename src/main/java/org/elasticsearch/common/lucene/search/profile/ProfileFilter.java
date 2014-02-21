package org.elasticsearch.common.lucene.search.profile;

import com.google.common.base.Stopwatch;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class ProfileFilter extends Filter implements ProfileComponent {

    private Filter subFilter;
    private long time = 0;

    public ProfileFilter(Filter filter) {
        this.subFilter = filter;
    }

    public Filter subFilter() {
        return this.subFilter;
    }

    public long time() {
        return this.time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void addTime(long time) {
        this.time += time;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DocIdSet idSet = this.subFilter.getDocIdSet(context, acceptDocs);
        stopwatch.stop();
        addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

        return idSet;
    }

    @Override
    public int hashCode() {
        int hash = 8;
        hash = 31 * hash + (this.subFilter.hashCode());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if ((obj == null) || (obj.getClass() != this.getClass()))
            return false;

        ProfileFilter other = (ProfileFilter) obj;
        return this.subFilter.equals(other.subFilter());
    }

    @Override
    public String toString() {
        return this.subFilter.toString();
    }


}
