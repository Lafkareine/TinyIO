package lafkareine.util.io;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class SerializeIO {


	public static void save(Path path, Serializable obj) throws IOException {

		Files.createDirectories(path);

		try (FileOutputStream stream1 = new FileOutputStream(path.toFile())) {
			ObjectOutputStream stream2 = new ObjectOutputStream(stream1);
			stream2.writeObject(obj);
		}
	}

    public static void saveUncheck(Path path, Serializable obj) {
        try {
            save(path, obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	public static <T extends Serializable> T read(Path path, Class<T> cls) throws ClassNotFoundException, IOException {
		if (Files.exists(path)) {
			try (FileInputStream stream1 = new FileInputStream(path.toFile())) {
				ObjectInputStream stream2 = new ObjectInputStream(stream1);
				Object o = stream2.readObject();
				return cls.cast(o);
			}
		} else {
			throw new FileNotFoundException();
		}
	}

    public static <T extends Serializable> T readUncheck(Path path, Class<T> cls) {
        try {
            return read(path,cls);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
