package org.elasticsearch.common.path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class FSTRepresentation<T> {

	/**
	 * Convert the value to a byte-array for storing
	 * @param value The value to be converted to bytes
	 * @return Returns the bytes of the given value, or null if something fails
	 */
	public byte[] toBytes(T value) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] ret = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(value);
			out.flush();
			ret = bos.toByteArray();
		} catch (IOException e) {
			try { bos.close(); } catch (IOException ex) { }
		} finally {
			try { bos.close(); } catch (IOException ex) { }
		}

		return ret;
	}

	/**
	 * Restore the stored byte-array into an object upon retrieval
	 * @param bytes The bytes to restore
	 * @return Returns the object represented by the bytes
	 */
	@SuppressWarnings("unchecked")
	public T fromBytes(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;

		T ret = null;
		try {
			in = new ObjectInputStream(bis);
			ret = (T) in.readObject();
		} catch (IOException | ClassNotFoundException e) {
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) { }
		}

		return ret;
	}

}
