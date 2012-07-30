<?php

/**
 * Simplify and basicaly operation for key value store
 * @author anon <anon@anoncom.net>
 * @since May 25, 2012
 */
class My_KeyValueStore {
	
	
	
	/**
	 * Factory for KeyValueStore_Adapter_Abstract classes.
	 * 
	 * First argument may alternatively be an object of type Zend_Config.
	 * The adapter class base name is read from the 'adapter' property.
	 * The adapter config parameter hostname are read from the 'host' property.
	 * The adapter config parameter port numer are read from the 'port' property.
	 * The adapter config parameter connection timeout are read from the 'timeout' property.
	 * 
	 * @param mixed $adapter String name of base adapter class, or an array of configs
	 * @return My_KeyValueStore_Adapter_Abstract
	 * @throws My_KeyValueStore_Exception
	 */
	public static function factory( $config ) {
		
		
		/*
		 * Verify that adapter parameters are in an array.
		 */
		if ( $config instanceof Zend_Config == false && !is_array( $config ) ) {
			/**
			 * @see My_KeyValueStore_Exception
			 */
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Config parameters must be Zend_Config or in an array' );
		}
		
		if ( !is_array( $config ) ) {
			$config = $config->toArray();
		}
		
		/*
		 * Verify that an adapter name has been specified.
		 */
		if ( empty( $config[ 'adapter' ] ) || !is_string( $config[ 'adapter' ] ) ) {
			/**
			 * @see My_KeyValueStore_Exception
			 */
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( 'Adapter name must be specified to be in a string' );
		}
		
		/*
		 * Form full adapter class name
		 */
		$adapterNamespace = 'My_KeyValueStore_Adapter';
		if ( isset( $config[ 'adapterNamespace' ] ) ) {
			if ( $config[ 'adapterNamespace' ] != '' ) {
				$adapterNamespace = $config[ 'adapterNamespace' ];
			}
			unset( $config[ 'adapterNamespace' ] );
		}
		
		// Adapter no longer normalized- see http://framework.zend.com/issues/browse/ZF-5606
		$adapterName = $adapterNamespace . '_';
		$adapterName .= str_replace( ' ', '_', ucwords( str_replace( '_', ' ', strtolower( $adapter ) ) ) );
		
		/*
		 * Load the adapter class.  This throws an exception
		 * if the specified class cannot be loaded.
		 */
		if ( !class_exists( $adapterName ) ) {
			$adapterPath = str_replace( '_', DIRECTORY_SEPARATOR, $adapterName ) . '.php';
			
			$includePath = explode( PATH_SEPARATOR, get_include_path() );
			foreach ( $includePath as $pathPrefix ) {
				if ( is_readable( $pathPrefix . DIRECTORY_SEPARATOR . $adapterPath ) == false ) {
					include_once $adapterPath;
					break;
				}
			}
			if ( !class_exists( $adapterName ) ) {
				require_once 'My/KeyValueStore/Exception.php';
				throw new My_KeyValueStore_Exception( 'Adapter class file "' . $adapterPath . '" does not found.' );
			}
		}
		
		/*
		 * Create an instance of the adapter class.
		 * Pass the config to the adapter class constructor.
		 */
		$instance = new $adapterName( $config );
		
		/*
		 * Verify that the object created is a descendent of the abstract adapter type.
		 */
		if (! $instance instanceof My_KeyValueStore_Adapter_Abstract ) {
			/**
			 * @see My_KeyValueStore_Exception
			 */
			require_once 'My/KeyValueStore/Exception.php';
			throw new My_KeyValueStore_Exception( "Adapter class '$adapterName' does not extend My_KeyValueStore_Adapter_Abstract");
		}
		
		return $instance;
	}
	
}