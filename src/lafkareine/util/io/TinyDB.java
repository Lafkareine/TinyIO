package lafkareine.util.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 文字と値のセットの低機能なファイルへの入出力を抽象化します
 * <p>
 * <p>
 * 複数の書き込みが同時に行われる場合、個別にsetを呼び出すのではなくtransactionを利用するほうが効率的です
 * また、値が変更された直後にファイルへの書き込みが行われます
 * 外部で値が変更された場合、reloadによって外部の変更を反映することができます
 */
public final class TinyDB {
	private final Path path;
	private final Path tmp;

	private final Map<String, String> DB = new HashMap<>();

	public TinyDB(Path base, String name) {

		path = base.resolve(name + ".tinydb");
		tmp = base.resolve(name + ".tmp");
		reload();
	}

	public Path getPath() {
		return path;
	}

	public TinyDB(Class base, String name) {
		this(ResourcePath.getJarPath(base), name);
	}

	public TinyDB(String name) {
		this(ResourcePath.getJarPath(), name);
	}

	private boolean isWritten;

	private boolean auto_save = true;

	public void setAutoSave(boolean auto_save) {
		this.auto_save = auto_save;
		if (auto_save && isWritten) {
			save();
		}
	}

	private static String escape(String before) {
		return before.replace("<eq>", "<<eq>>").replace("=", "<eq>").replace("<cr>", "<<cr>>").replace("\r", "<cr>").replace("<lf>", "<<lf>>").replace("\n", "<lf>");
	}

	private static String unescape(String before) {
		return before.replace("<lf>", "\n").replace("<\n>", "<lf>").replace("<cr>", "\r").replace("<\r>", "<cr>").replace("<eq>", "=").replace("<=>", "<eq>");
	}

	private void saveIfAuto() {
		isWritten = true;
		if (auto_save) {
			save();
		}
	}

	public void save() {
		if (isWritten) {
			try {
				Files.createDirectories(path.getParent());
				Files.write(tmp, getScript().getBytes());
				Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
				isWritten = false;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void transaction(Consumer<TinyDB> action) {
		boolean temp_save = auto_save;
		auto_save = false;
		action.accept(this);
		auto_save = temp_save;
		if (auto_save) {
			save();
		}
	}

	public void remove(String name) {
		if (name == null) {
			throw new IllegalArgumentException("name can't null");
		}
		DB.remove(name);
		saveIfAuto();
	}

	public void remove(String... names) {
		transaction(x -> {
			for (String e : names) {
				x.remove(e);
			}
		});
	}

	public void clear() {
		DB.clear();
		saveIfAuto();
	}

	public void reload() {
		DB.clear();
		if (Files.exists(path)) {
			try {
				String script = Files.readString(path);
				script.lines().forEach(x -> {
					String[] info = x.split("=");
					DB.put(info[0], info[1]);
				});
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public Optional<String> get(String name) {
		String value = DB.get(name);
		return (value != null) ? Optional.of(unescape(value)) : Optional.empty();
	}

	public Optional<Boolean> getBool(String name) {
		String value = DB.get(name);
		return (value != null) ? Optional.of(Boolean.parseBoolean(value)) : Optional.empty();
	}

	public Optional<Byte> getByte(String name) {
		String value = DB.get(name);
		return (value != null) ? Optional.of(Byte.parseByte(value)) : Optional.empty();
	}

	public Optional<Short> getShort(String name) {
		String value = DB.get(name);
		return (value != null) ? Optional.of(Short.parseShort(value)) : Optional.empty();

	}

	public OptionalInt getInt(String name) {
		String value = DB.get(name);
		return (value != null) ? OptionalInt.of(Integer.parseInt(value)) : OptionalInt.empty();
	}

	public OptionalLong getLong(String name) {
		String value = DB.get(name);
		return (value != null) ? OptionalLong.of(Long.parseLong(value)) : OptionalLong.empty();
	}

	public Optional<Float> getFloat(String name) {
		String value = DB.get(name);
		return (value != null) ? Optional.of(Float.parseFloat(value)) : Optional.empty();
	}

	public OptionalDouble getDouble(String name) {
		String value = DB.get(name);
		return (value != null) ? OptionalDouble.of(Double.parseDouble(value)) : OptionalDouble.empty();
	}

	private String[] getRawArray(String name) {
		String script = DB.get(name);
		return (script != null) ? script.split(",") : null;
	}

	public Optional<String[]> getArray(String name) {
		String[] script = getRawArray(name);
		return (script != null) ? Optional.of(Arrays.stream(script).map(x -> unescape(x).replace("<cm>", ",").replace("<,>", "<cm>")).toArray(String[]::new)) : Optional.empty();
	}

	private Stream<String> getAsStream(String name) {
		String script = DB.get(name);
		return (script != null) ? Arrays.stream(script.split(",")) : null;
	}

	public Optional<boolean[]> getBoolArray(String name) {
		String[] scripts = getRawArray(name);
		if (scripts != null) {
			boolean[] data = new boolean[scripts.length];
			for (int i = 0; i < scripts.length; i++) {
				data[i] = Boolean.parseBoolean(scripts[i]);
			}
			return Optional.of(data);
		} else {
			return Optional.empty();
		}
	}

	public Optional<byte[]> getByteArray(String name) {
		String[] scripts = getRawArray(name);
		if (scripts != null) {
			byte[] data = new byte[scripts.length];
			for (int i = 0; i < scripts.length; i++) {
				data[i] = Byte.parseByte(scripts[i]);
			}
			return Optional.of(data);
		} else {
			return Optional.empty();
		}
	}

	public Optional<short[]> getShortArray(String name) {
		String[] scripts = getRawArray(name);
		if (scripts != null) {
			short[] data = new short[scripts.length];
			for (int i = 0; i < scripts.length; i++) {
				data[i] = Short.parseShort(scripts[i]);
			}
			return Optional.of(data);
		} else {
			return Optional.empty();
		}
	}

	public Optional<int[]> getIntArray(String name) {
		Stream<String> stream = getAsStream(name);
		if (stream != null) {
			return Optional.of(stream.mapToInt(x -> Integer.parseInt(x)).toArray());
		} else {
			return null;
		}
	}

	public Optional<long[]> getLongArray(String name) {
		Stream<String> stream = getAsStream(name);
		if (stream != null) {
			return Optional.of(stream.mapToLong(x -> Long.parseLong(x)).toArray());
		} else {
			return null;
		}
	}

	public Optional<float[]> getFloatArray(String name) {
		String[] scripts = getRawArray(name);
		if (scripts != null) {
			float[] data = new float[scripts.length];
			for (int i = 0; i < scripts.length; i++) {
				data[i] = Float.parseFloat(scripts[i]);
			}
			return Optional.of(data);
		} else {
			return Optional.empty();
		}
	}

	public Optional<double[]> getDoubleArray(String name) {
		Stream<String> stream = getAsStream(name);
		if (stream != null) {
			return Optional.of(stream.mapToDouble(x -> Double.parseDouble(x)).toArray());
		} else {
			return null;
		}
	}

	public String getScript() {
		List<Map.Entry<String, String>> entries = DB.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toUnmodifiableList());
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, String> e : entries) {
			builder.append(e.getKey()).append('=').append(e.getValue()).append(System.lineSeparator());
		}
		return builder.toString();
	}

	public String getOrDefault(String name, String default_value) {
		Optional<String> value = get(name);
		if (value.isPresent()) {
			return value.get();
		} else {
			setRaw(name, default_value);
			return default_value;
		}
	}

	public boolean getBoolOrDefault(String name, boolean default_value) {
		return Boolean.parseBoolean(getOrDefault(name, Boolean.toString(default_value)));
	}

	public byte getByteOrDefault(String name, byte default_value) {
		return Byte.parseByte(getOrDefault(name, Byte.toString(default_value)));
	}

	public short getShortOrDefault(String name, short default_value) {
		return Short.parseShort(getOrDefault(name, Short.toString(default_value)));
	}

	public int getIntOrDefault(String name, int default_value) {
		return Integer.parseInt(getOrDefault(name, Integer.toString(default_value)));
	}

	public long getLongOrDefault(String name, long default_value) {
		return Long.parseLong(getOrDefault(name, Long.toString(default_value)));
	}

	public float getFloatOrDefault(String name, float default_value) {
		return Float.parseFloat(getOrDefault(name, Float.toString(default_value)));
	}

	public double getDoubleOrDefault(String name, double default_value) {
		return Double.parseDouble(getOrDefault(name, Double.toString(default_value)));
	}

	public String[] getArrayOrDefault(String name, String... default_value) {
		Optional<String[]> optional = getArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public boolean[] getBoolArrayOrDefault(String name, boolean[] default_value) {
		Optional<boolean[]> optional = getBoolArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public byte[] getByteArrayOrDefault(String name, byte[] default_value) {
		Optional<byte[]> optional = getByteArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public short[] getShortArrayOrDefault(String name, short[] default_value) {
		Optional<short[]> optional = getShortArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public int[] getIntArrayOrDefault(String name, int[] default_value) {
		Optional<int[]> optional = getIntArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public long[] getLongArrayOrDefault(String name, long[] default_value) {
		Optional<long[]> optional = getLongArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public float[] getFloatArrayOrDefault(String name, float[] default_value) {
		Optional<float[]> optional = getFloatArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public double[] getDoubleArrayOrDefault(String name, double[] default_value) {
		Optional<double[]> optional = getDoubleArray(name);
		if (optional.isPresent()) {
			return optional.get();
		} else {
			set(name, default_value);
			return default_value;
		}
	}

	public void set(String script) {
		transaction(x -> {
			String[] entry = script.lines().toArray(String[]::new);
			for (String e : entry) {
				String[] split = e.split("=");
				x.setRaw(split[0], split[1]);
			}
		});
	}

	private void setRaw(String name, String value) {
		if (name == null || value == null) {
			throw new IllegalArgumentException("name and value can't null");
		}
		if (name.indexOf("=") != -1 || name.indexOf("\r") != -1 || name.indexOf("\n") != -1) {
			throw new IllegalArgumentException("invalid name \"" + name + "\"");
		}
		DB.put(name, value);
		saveIfAuto();
	}

	public void set(String name, String value) {
		setRaw(name, escape(value));
	}

	public void set(String name, boolean value) {
		setRaw(name, Boolean.toString(value));
	}

	public void set(String name, byte value) {
		setRaw(name, Byte.toString(value));
	}

	public void set(String name, short value) {
		setRaw(name, Short.toString(value));
	}

	public void set(String name, int value) {
		setRaw(name, Integer.toString(value));
	}

	public void set(String name, long value) {
		setRaw(name, Long.toString(value));
	}

	public void set(String name, float value) {
		setRaw(name, Float.toString(value));
	}

	public void set(String name, double value) {
		setRaw(name, Double.toString(value));
	}

	public void set(String name, String... array) {
		int size = 0;
		for (String e : array) {
			size += e.length();
		}
		size += array.length - 1;
		StringBuilder builder = new StringBuilder(size);
		int cnm = array.length - 1;
		for (String e : array) {
			builder.append(escape(e).replace("<cm>", "<<cm>>").replace(",", "<cm>"));
			if (cnm-- > 0) builder.append(',');
		}
		setRaw(name, builder.toString());
	}

	public void set(String name, boolean[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	public void set(String name, byte[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	public void set(String name, short[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	public void set(String name, int[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	public void set(String name, long[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	public void set(String name, float[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	public void set(String name, double[] array) {
		String temp = Arrays.toString(array).replace(" ", "");
		setRaw(name, temp.substring(1, temp.length() - 1));
	}

	@Override
	public String toString() {
		return "TinyDB{" +
				"path=" + path +
				", tmp=" + tmp +
				", DB=" + DB +
				", isWritten=" + isWritten +
				", auto_save=" + auto_save +
				System.lineSeparator() + "data:" + System.lineSeparator() +
				toString() +
				'}';
	}
}
