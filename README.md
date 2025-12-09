# csdl-cot

A Rust project for column-oriented storage and decoding.

## Overview
This project implements a columnar storage engine with efficient decoding mechanisms. It is organized into several modules for catalog management and storage operations.

## Project Structure
- `src/`
  - `main.rs`: Entry point of the application.
  - `lib.rs`: Library root.
  - `catalog/`: Catalog management module.
  - `storage/`: Storage and column decoding modules.
- `Cargo.toml`: Project configuration and dependencies.

## Getting Started
### Prerequisites
- Rust (https://www.rust-lang.org/tools/install)

### Build
```sh
cargo build
```

### Run
```sh
cargo run
```

### Test
```sh
cargo test
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License