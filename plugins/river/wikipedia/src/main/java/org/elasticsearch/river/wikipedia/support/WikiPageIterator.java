package org.elasticsearch.river.wikipedia.support;

import java.util.Vector;

/**
 * A class to iterate the pages after the wikipedia XML file has been parsed with {@link WikiXMLDOMParser}.
 *
 * @author Delip Rao
 * @see WikiXMLDOMParser
 */
public class WikiPageIterator {

    private int currentPage = 0;
    private int lastPage = 0;
    Vector<WikiPage> pageList = null;

    public WikiPageIterator(Vector<WikiPage> list) {
        pageList = list;
        if (pageList != null)
            lastPage = pageList.size();
    }

    /**
     * @return true if there are more pages to be read
     */
    public boolean hasMorePages() {
        return (currentPage < lastPage);
    }

    /**
     * Reset the iterator.
     */
    public void reset() {
        currentPage = 0;
    }

    /**
     * Advances the iterator by one position.
     *
     * @return a {@link WikiPage}
     */
    public WikiPage nextPage() {
        if (hasMorePages())
            return pageList.elementAt(currentPage++);
        return null;
	}
}
