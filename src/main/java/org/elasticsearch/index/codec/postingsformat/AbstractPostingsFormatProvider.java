package org.elasticsearch.index.codec.postingsformat;

/**
 */
public abstract class AbstractPostingsFormatProvider implements PostingsFormatProvider {

    private final String name;

    protected AbstractPostingsFormatProvider(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

}
