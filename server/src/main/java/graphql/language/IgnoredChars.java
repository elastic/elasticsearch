package graphql.language;

import graphql.PublicApi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@PublicApi
public class IgnoredChars implements Serializable {

    private final List<IgnoredChar> left;
    private final List<IgnoredChar> right;

    public static final IgnoredChars EMPTY = new IgnoredChars(Collections.emptyList(), Collections.emptyList());

    public IgnoredChars(List<IgnoredChar> left, List<IgnoredChar> right) {
        this.left = new ArrayList<>(left);
        this.right = new ArrayList<>(right);
    }


    public List<IgnoredChar> getLeft() {
        return new ArrayList<>(left);
    }

    public List<IgnoredChar> getRight() {
        return new ArrayList<>(right);
    }
}
