package graphql.execution;

import graphql.Assert;
import graphql.AssertException;
import graphql.PublicApi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;
import static java.lang.String.format;


/**
 * As a graphql query is executed, each field forms a hierarchical path from parent field to child field and this
 * class represents that path as a series of segments.
 */
@PublicApi
public class ExecutionPath {
    private static final ExecutionPath ROOT_PATH = new ExecutionPath();

    /**
     * All paths start from here
     *
     * @return the root path
     */
    public static ExecutionPath rootPath() {
        return ROOT_PATH;
    }

    private final ExecutionPath parent;
    private final PathSegment segment;
    private final List<Object> pathList;

    private ExecutionPath() {
        parent = null;
        segment = null;
        pathList = toListImpl();
    }

    private ExecutionPath(ExecutionPath parent, PathSegment segment) {
        this.parent = assertNotNull(parent, "Must provide a parent path");
        this.segment = assertNotNull(segment, "Must provide a sub path");
        pathList = toListImpl();
    }

    public int getLevel() {
        int counter = 0;
        ExecutionPath currentPath = this;
        while (currentPath != null) {
            if (currentPath.segment instanceof StringPathSegment) {
                counter++;
            }
            currentPath = currentPath.parent;
        }
        return counter;
    }

    public ExecutionPath getPathWithoutListEnd() {
        if (ROOT_PATH.equals(this)) {
            return ROOT_PATH;
        }
        if (segment instanceof StringPathSegment) {
            return this;
        }
        return parent;
    }

    public String getSegmentName() {
        if (segment instanceof StringPathSegment) {
            return ((StringPathSegment) segment).getValue();
        } else {
            if (parent == null) {
                return null;
            }
            return ((StringPathSegment) parent.segment).getValue();
        }
    }

    /**
     * Parses an execution path from the provided path string in the format /segment1/segment2[index]/segmentN
     *
     * @param pathString the path string
     *
     * @return a parsed execution path
     */
    public static ExecutionPath parse(String pathString) {
        pathString = pathString == null ? "" : pathString;
        pathString = pathString.trim();
        StringTokenizer st = new StringTokenizer(pathString, "/[]", true);
        ExecutionPath path = ExecutionPath.rootPath();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if ("/".equals(token)) {
                assertTrue(st.hasMoreTokens(), mkErrMsg(), pathString);
                path = path.segment(st.nextToken());
            } else if ("[".equals(token)) {
                assertTrue(st.countTokens() >= 2, mkErrMsg(), pathString);
                path = path.segment(Integer.parseInt(st.nextToken()));
                String closingBrace = st.nextToken();
                assertTrue(closingBrace.equals("]"), mkErrMsg(), pathString);
            } else {
                throw new AssertException(format(mkErrMsg(), pathString));
            }
        }
        return path;
    }

    /**
     * This will create an execution path from the list of objects
     *
     * @param objects the path objects
     *
     * @return a new execution path
     */
    public static ExecutionPath fromList(List<?> objects) {
        assertNotNull(objects);
        ExecutionPath path = ExecutionPath.rootPath();
        for (Object object : objects) {
            if (object instanceof Number) {
                path = path.segment(((Number) object).intValue());
            } else {
                path = path.segment(String.valueOf(object));
            }
        }
        return path;
    }

    private static String mkErrMsg() {
        return "Invalid path string : '%s'";
    }

    /**
     * Takes the current path and adds a new segment to it, returning a new path
     *
     * @param segment the string path segment to add
     *
     * @return a new path containing that segment
     */
    public ExecutionPath segment(String segment) {
        return new ExecutionPath(this, new StringPathSegment(segment));
    }

    /**
     * Takes the current path and adds a new segment to it, returning a new path
     *
     * @param segment the int path segment to add
     *
     * @return a new path containing that segment
     */
    public ExecutionPath segment(int segment) {
        return new ExecutionPath(this, new IntPathSegment(segment));
    }

    public ExecutionPath sibling(String siblingField) {
        Assert.assertTrue(!ROOT_PATH.equals(this), "You MUST not call this with the root path");
        return new ExecutionPath(this.parent, new StringPathSegment(siblingField));
    }

    /**
     * @return converts the path into a list of segments
     */
    public List<Object> toList() {
        return new ArrayList<>(pathList);
    }

    private List<Object> toListImpl() {
        if (parent == null) {
            return Collections.emptyList();
        }
        List<Object> list = new ArrayList<>();
        ExecutionPath p = this;
        while (p.segment != null) {
            list.add(p.segment.getValue());
            p = p.parent;
        }
        Collections.reverse(list);
        return list;
    }

    /**
     * @return the path as a string which represents the call hierarchy
     */
    @Override
    public String toString() {
        if (parent == null) {
            return "";
        }

        if (ROOT_PATH.equals(parent)) {
            return segment.toString();
        }

        return parent.toString() + segment.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutionPath that = (ExecutionPath) o;

        return pathList.equals(that.pathList);
    }

    @Override
    public int hashCode() {
        return pathList.hashCode();
    }

    public interface PathSegment<T> {
        T getValue();
    }

    private static class StringPathSegment implements PathSegment<String> {
        private final String value;

        StringPathSegment(String value) {
            assertTrue(value != null && !value.isEmpty(), "empty path component");
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return '/' + value;
        }
    }

    private static class IntPathSegment implements PathSegment<Integer> {
        private final int value;

        IntPathSegment(int value) {
            this.value = value;
        }

        @Override
        public Integer getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "[" + value + ']';
        }
    }
}
