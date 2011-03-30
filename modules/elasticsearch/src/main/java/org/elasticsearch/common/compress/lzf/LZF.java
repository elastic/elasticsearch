/* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.elasticsearch.common.compress.lzf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Simple command-line utility that can be used for testing LZF
 * compression, or as rudimentary command-line tool.
 * Arguments are the same as used by the "standard" lzf command line tool
 *
 * @author tatu@ning.com
 */
public class LZF {

    public static boolean isCompressed(final byte[] buffer) {
        return buffer.length >= 2 && buffer[0] == LZFChunk.BYTE_Z && buffer[1] == LZFChunk.BYTE_V;
    }

    public final static String SUFFIX = ".lzf";

    void process(String[] args) throws IOException {
        if (args.length == 2) {
            String oper = args[0];
            boolean compress = "-c".equals(oper);
            if (compress || "-d".equals(oper)) {
                String filename = args[1];
                File src = new File(filename);
                if (!src.exists()) {
                    System.err.println("File '" + filename + "' does not exist.");
                    System.exit(1);
                }
                if (!compress && !filename.endsWith(SUFFIX)) {
                    System.err.println("File '" + filename + "' does end with expected suffix ('" + SUFFIX + "', won't decompress.");
                    System.exit(1);
                }
                byte[] data = readData(src);
                System.out.println("Read " + data.length + " bytes.");
                byte[] result = compress ? LZFEncoder.encode(data) : LZFDecoder.decode(data);
                System.out.println("Processed into " + result.length + " bytes.");
                File resultFile = compress ? new File(filename + SUFFIX) : new File(filename.substring(0, filename.length() - SUFFIX.length()));
                FileOutputStream out = new FileOutputStream(resultFile);
                out.write(result);
                out.close();
                System.out.println("Wrote in file '" + resultFile.getAbsolutePath() + "'.");
                return;
            }
        }
        System.err.println("Usage: java " + getClass().getName() + " -c/-d file");
        System.exit(1);
    }

    private byte[] readData(File in) throws IOException {
        int len = (int) in.length();
        byte[] result = new byte[len];
        int offset = 0;
        FileInputStream fis = new FileInputStream(in);

        while (len > 0) {
            int count = fis.read(result, offset, len);
            if (count < 0) break;
            len -= count;
            offset += count;
        }
        fis.close();
        if (len > 0) { // should never occur...
            throw new IOException("Could not read the whole file -- received EOF when there was " + len + " bytes left to read");
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        new LZF().process(args);
    }
}

