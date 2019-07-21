package graphql.parser;

import graphql.Assert;
import graphql.PublicApi;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This reader allows you to read N number readers and combine them as one logical reader
 * however you can then map back to the underlying readers in terms of their source name
 * and the relative lines numbers.
 *
 * It can also track all data in memory if you want to have all of the previous read data in
 * place at some point in time.
 */
@PublicApi
public class MultiSourceReader extends Reader {

    private final List<SourcePart> sourceParts;
    private final StringBuilder data = new StringBuilder();
    private int currentIndex = 0;
    private int overallLineNumber = 0;
    private final boolean trackData;


    private MultiSourceReader(Builder builder) {
        this.sourceParts = builder.sourceParts;
        this.trackData = builder.trackData;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        while (true) {
            synchronized (this) {
                if (currentIndex >= sourceParts.size()) {
                    return -1;
                }
                SourcePart sourcePart = sourceParts.get(currentIndex);
                int read = sourcePart.lineReader.read(cbuf, off, len);
                overallLineNumber = calcLineNumber();
                if (read == -1) {
                    currentIndex++;
                } else {
                    trackData(cbuf, off, read);
                    return read;
                }
            }
        }
    }

    private void trackData(char[] cbuf, int off, int len) {
        if (trackData) {
            data.append(cbuf, off, len);
        }
    }

    private int calcLineNumber() {
        int linenumber = 0;
        for (SourcePart sourcePart : sourceParts) {
            linenumber += sourcePart.lineReader.getLineNumber();
        }
        return linenumber;
    }

    public static class SourceAndLine {
        private String sourceName = null;
        private int line = 0;

        public String getSourceName() {
            return sourceName;
        }

        public int getLine() {
            return line;
        }

        @Override
        public String toString() {
            return "SourceAndLine{" +
                    "sourceName='" + sourceName + '\'' +
                    ", line=" + line +
                    '}';
        }
    }

    /**
     * This returns the source name and line number given an overall line number
     *
     * This is zeroes based like {@link java.io.LineNumberReader#getLineNumber()}
     *
     * @param overallLineNumber the over all line number
     *
     * @return the source name and relative line number to that source
     */
    public SourceAndLine getSourceAndLineFromOverallLine(int overallLineNumber) {
        SourceAndLine sourceAndLine = new SourceAndLine();
        if (sourceParts.isEmpty()) {
            return sourceAndLine;
        }
        if (overallLineNumber == 0) {
            sourceAndLine.sourceName = sourceParts.get(0).sourceName;
            sourceAndLine.line = 0;
            return sourceAndLine;
        }
        SourcePart currentPart;
        if (currentIndex >= sourceParts.size()) {
            currentPart = sourceParts.get(sourceParts.size() - 1);
        } else {
            currentPart = sourceParts.get(currentIndex);
        }
        int page = 0;
        int previousPage;
        for (SourcePart sourcePart : sourceParts) {
            sourceAndLine.sourceName = sourcePart.sourceName;
            if (sourcePart == currentPart) {
                // we cant go any further
                int partLineNumber = currentPart.lineReader.getLineNumber();
                previousPage = page;
                page += partLineNumber;
                if (page > overallLineNumber) {
                    sourceAndLine.line = overallLineNumber - previousPage;
                } else {
                    sourceAndLine.line = page;
                }
                return sourceAndLine;
            } else {
                previousPage = page;
                int partLineNumber = sourcePart.lineReader.getLineNumber();
                page += partLineNumber;
                if (page > overallLineNumber) {
                    sourceAndLine.line = overallLineNumber - previousPage;
                    return sourceAndLine;
                }
            }
        }
        sourceAndLine.line = overallLineNumber - page;
        return sourceAndLine;
    }

    /**
     * @return the line number of the current source.  This is zeroes based like {@link java.io.LineNumberReader#getLineNumber()}
     */
    public int getLineNumber() {
        synchronized (this) {
            if (sourceParts.isEmpty()) {
                return 0;
            }
            if (currentIndex >= sourceParts.size()) {
                return sourceParts.get(sourceParts.size() - 1).lineReader.getLineNumber();
            }
            return sourceParts.get(currentIndex).lineReader.getLineNumber();
        }
    }

    /**
     * @return The name of the current source
     */
    public String getSourceName() {
        synchronized (this) {
            if (sourceParts.isEmpty()) {
                return null;
            }
            if (currentIndex >= sourceParts.size()) {
                return sourceParts.get(sourceParts.size() - 1).sourceName;
            }
            return sourceParts.get(currentIndex).sourceName;
        }
    }

    /**
     * @return the overall line number of the all the sources.  This is zeroes based like {@link java.io.LineNumberReader#getLineNumber()}
     */
    public int getOverallLineNumber() {
        return overallLineNumber;
    }

    public List<String> getData() {
        LineNumberReader reader = new LineNumberReader(new StringReader(data.toString()));
        List<String> lines = new ArrayList<>();
        while (true) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    return lines;
                }
                lines.add(line);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            for (SourcePart sourcePart : sourceParts) {
                if (!sourcePart.closed) {
                    sourcePart.lineReader.close();
                    sourcePart.closed = true;
                }
            }
        }
    }

    private static class SourcePart {
        String sourceName;
        LineNumberReader lineReader;
        boolean closed;
    }


    public static Builder newMultiSourceReader() {
        return new Builder();
    }

    public static class Builder {
        List<SourcePart> sourceParts = new ArrayList<>();
        boolean trackData = true;

        private Builder() {
        }

        public Builder reader(Reader reader, String sourceName) {
            SourcePart sourcePart = new SourcePart();
            sourcePart.lineReader = new LineNumberReader(Assert.assertNotNull(reader));
            sourcePart.sourceName = sourceName;
            sourcePart.closed = false;
            sourceParts.add(sourcePart);
            return this;
        }

        public Builder string(String input, String sourceName) {
            SourcePart sourcePart = new SourcePart();
            sourcePart.lineReader = new LineNumberReader(new StringReader(input));
            sourcePart.sourceName = sourceName;
            sourcePart.closed = false;
            sourceParts.add(sourcePart);
            return this;
        }

        public Builder trackData(boolean trackData) {
            this.trackData = trackData;
            return this;

        }

        public MultiSourceReader build() {
            return new MultiSourceReader(this);
        }
    }

}
