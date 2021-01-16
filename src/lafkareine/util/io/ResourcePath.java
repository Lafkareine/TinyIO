package lafkareine.util.io;

import java.net.URISyntaxException;
import java.nio.file.Path;

public class ResourcePath {
    public static Path getJarPath(Class target){
        return getDefaultPackagePath(target).getParent();
    }

    public static Path getDefaultPackagePath(Class target){
        String package_name = target.getPackage().getName();
        int depth = (package_name.length() == 0)?0:package_name.split("\\.").length;
        Path tmpPath;
        try {
            tmpPath = Path.of(target.getResource("").toURI());
            for (int i = 0; i < depth; i++) {
                tmpPath = tmpPath.getParent();
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return tmpPath;
    }

    public static Path getJarPath(){
        return getJarPath(ResourcePath.class);
    }

    public static Path getDefaultPackagePath(){
        return getDefaultPackagePath(ResourcePath.class);
    }
}
