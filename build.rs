use std::io::Result;
fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=protos/krpc.proto");
    println!("cargo:rerun-if-changed=src/krpc.rs");
    prost_build::compile_protos(&["protos/krpc.proto"], &["protos"])?;
    protoc_rust::Codegen::new()
        .out_dir("src/")
        .inputs(&["protos/krpc.proto"])
        .run()
        .expect("protoc");
    Ok(())
}
