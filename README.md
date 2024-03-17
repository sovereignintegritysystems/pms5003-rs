# PMS5003

This is a Rust interface to the [Plantower PMS5003](https://www.plantower.com/en/products_33/74.html) particulate air quality monitor.

The PMS5003 is ubiqutious in air quality monitoring devices, capable of measuring particle counts in varying sizes and reporting via a serial interface. The sensor can easily be connected to a Raspberry Pi UART via a breadboard adaptor, which can be sourced from a number of vendors, such as [Adafruit](https://www.adafruit.com/product/3686).

Since the data from the sensor is a continous stream, this crate provides a simplified interface that will catch a frame marker and then return an async stream of measurements from the sensor.  You can also utilize the `aqi` module to wrap this interface and obtain summary snapshots of the data, which provide instantaneous [AQI](https://en.wikipedia.org/wiki/Air_quality_index) computations. Note that the interpretation and usage of AQI data varies by country, and is intended to be averaged over certain periods of time.  The AQI monitor can be configured for different granularity (e.g., 1-minute) and retention times (e.g., one day). One-minute measurements are common on many sites that report air quality.

## Disclaimer

* The data provided by this crate is for informational purposes only and should not be used for critical decision-making, especially in medical settings.
* No warranty is made regarding the accuracy, completeness, or timeliness of the data.
* You are solely responsible for verifying the data and ensuring its suitability for your specific application.
* Using unverified data in a medical setting could lead to adverse health outcomes.
* Software is provided "as is" and, and author is not liable for any damages arising from its use.
