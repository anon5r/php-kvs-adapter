Various Key Value Store common interface for PHP
=========================

It provides a common interface to KVS simply in a variety for PHP.

# Installation

You can put this files on your system path of the configured "include_path" in php.ini .


# How to use

    $kvs = Recs_Apps_KeyValueStore::factory( $configs );

## SET the value for key name
If you want to set $value to key name of <KeyName> and result by boolean.

$value  = mixed value.
$exp    = OPTIONAL; expire time  by seconds (integer). default is 0 ( disabled expire).
$result = boolean, when success to set returns true, or when failed returns false.
    
    $result = $kvs->set<KeyName>( $value, $exp );


## GET the value by key name
If you want to get $value from key name of <KeyName>

$value = mixed value when you set before, or failed, returns false.
    
    $value = $kvs->get<KeyName>();

If you specify the first parameter, you can chose value in <KeyName> values.
However this option only enabled when the value of array.



## APPEND the value for key name
If you want to apped $value to existing key name of <KeyName>, You can append the value specified it.

$value  = mixed value.
$exp    = OPTIONAL; expire time  by seconds (integer). default is 0 ( disabled expire).
$result = boolean, when success to set returns true, or when failed returns false.


## REMOVE the specified index of values into key name
If you want to remove the specified index of values into existing key name of <KeyName>.

$index  = index key of values.
$result = boolean, when success to set returns true, or when failed returns false.

    $result $kvs->remove<KeyName>( $index );



## PULL the specified index of values into key name
If you want to pull ( to get and remove the value ) the specified index of values into existing key name of <KeyName>.

$index  = index key of values.
$value  = mixed value when you set before, or failed, returns false.

    $result $kvs->pull<KeyName>( $index );


## INCREMENT the value to key name
If you want increment value to existing key name of <KeyName>.

$index  = index key of values.
$result = boolean, when success to set returns true, or when failed returns false.

    $result $kvs->increment<KeyName>( $index );



## DECREMENT the value from key name
If you want decrement from existing key name of <KeyName>.

$value  = decremented value

    $result $kvs->decrement<KeyName>( $index );

