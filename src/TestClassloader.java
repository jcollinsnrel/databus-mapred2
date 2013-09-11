import java.net.URL;
import java.net.URLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClassloader extends URLClassLoader {

	static final Logger log = LoggerFactory.getLogger(TestClassloader.class);

	private String clname = "";

	public TestClassloader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
	}

	public void setName(String name) {
		clname = name;
	}

	protected Class<?> findClass(final String name) throws ClassNotFoundException {
		log.debug("calling findClass with class " + name + ".  I am " + this);
		Class<?> ret = super.findClass(name);
		log.debug("found class " + name + " I am " + name + " it's classloader is " + ret.getClassLoader());
		return ret;
	}

	public Class<?> loadClass(String name) throws ClassNotFoundException {
		log.debug("calling loadClass with class " + name + ".  I am " + this);
		Class<?> ret = super.loadClass(name);
		log.debug("found class " + name + ".  I am " + this + " it's classloader is " + ret.getClassLoader() + " it's codesource it " + ret.getProtectionDomain().getCodeSource());

		return ret;
	}

	public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		log.debug("calling loadClass boolean with class " + name + ".  I am " + this);

		// reverse the logic. try myself first, then delgate:
		Class c = findLoadedClass(name);
		if (c == null) {
			try {
				c = findClass(name);
				log.debug("Yes!  classloader" + this + " did have " + name);
			} catch (ClassNotFoundException cnfe) {
				log.debug("No!  classloader" + this + " didn't have " + name + " delegating to parent!");
				c = super.loadClass(name, resolve);
			}
		}
		if (resolve) {
			resolveClass(c);
		}
		log.debug("found class " + name + ".  I am " + this + " it's classloader is " + c.getClassLoader() + " it's codesource it " + c.getProtectionDomain().getCodeSource());

		return c;
	}

	@Override
	public String toString() {
		return "TestClassloader named " + clname + " super: " + super.toString();
	}

}
