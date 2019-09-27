'use strict'
const mongoose = require('mongoose')
const Float = require('mongoose-float').loadType(mongoose)
const fs = require('fs')
const csv = require('fast-csv')
const express = require('express')
const app = express()

mongoose.connect('mongodb://localhost:27017/banco_dev', {
    useNewUrlParser: true,
    useUnifiedTopology: true
})

const sellModel = mongoose.model('sell', new mongoose.Schema({
    num_ped: String,
    seq_ped: String,
    client: String,
    seller: String,
    status: String,
    dt_sell: Date,
    cat_prod: String,
    fam_prod: String,
    cod_prod: String,
    sell_value: Float,
    sell_quant: Float,
    oc: String
}))

const calculateSellQuantity = (qntCX, qtdUND, qntUN, retCX, retUN) => {
    const total_qnt = (parseFloat(qntCX).toFixed(2) * parseFloat(qtdUND).toFixed(2) + parseFloat(qntUN).toFixed(2)) / parseFloat(qtdUND).toFixed(2)
    const total_ret = (parseFloat(retCX).toFixed(2) * parseFloat(qtdUND).toFixed(2) + parseFloat(retUN).toFixed(2)) / parseFloat(qtdUND).toFixed(2)
    return total_qnt - total_ret
}

const calculateSellValue = (totalValue, returnValue) => {
    const total_value = parseFloat(totalValue).toFixed(2)
    const return_value = parseFloat(returnValue).toFixed(2)
    return total_value - return_value
}

const toDate = (dateStr) => {
    const [day, month, year] = dateStr.split("/")
    return new Date(year, month - 1, day)
}

const removeDuplcates = (arr) => arr.filter((este, i) => arr.indexOf(este) === i)

if(checkVolume_600_300_1lt(pedido)) volume_300_600_1lt += pedido.sell_quant
if(checkVolume_premium(pedido)) volume_premium += pedido.sell_quant
if(checkVolume_volume_total(pedido)) volume_volume_total += pedido.sell_quant
if(checkKVolume_curinga01(pedido)) volume_curinga01 += pedido.sell_quant
if(checkKVolume_curinga02(pedido)) volume_curinga02 += pedido.sell_quant

const checkVolume_600_300_1lt = (sell) => {
    let isValid = false
    const fam = ['001','004','025','061','062','081']
    fam.map(element => { if(element === sell.fam_prod) {isValid = true} })
    return isValid
}

const checkVolume_premium = (sell) => {
    let isValid = false
    const fam = ['010','081','025','061','062','081']
    fam.map(element => { if(element === sell.fam_prod) {isValid = true} })
    return isValid
}

const checkCoberturaDevassa600 = (sell) => {
    let isValid = false
    const fam = ['0000903061']
    fam.map(element => { if(element === sell.cod_prod) {isValid = true} })
    return isValid
}

const checkCoberturaDevassaTotal = (sell) => {
    let isValid = false
    const fam = ['0000903061','0000901161','0000903062', '0000903130', '0000903129']
    fam.map(element => { if(element === sell.cod_prod) {isValid = true} })
    return isValid
}

const checkCoberturaEisenbahn600 = (sell) => {
    let isValid = false
    const fam = ['0000903024','0000903025']
    fam.map(element => { if(element === sell.cod_prod) {isValid = true} })
    return isValid
}

const checkCoberturaEisenbahnTotal = (sell) => {
    let isValid = false
    const fam = ['0000903024','0000901781','0000903212', '0000903561', '0000900820', '0000902241', '0000902544', '0000903025']
    fam.map(element => { if(element === sell.cod_prod) {isValid = true} })
    return isValid
}

const checkCoberturaSchin600 = (sell) => {
    let isValid = false
    const fam = ['0000902411']
    fam.map(element => { if(element === sell.cod_prod) {isValid = true} })
    return isValid
}

const checkCoberturaSchinTotal = (sell) => {
    let isValid = false
    const fam = ['0000902411', '0000902406', '0000902410', '0000902432', '0000902452', '0000902451']
    fam.map(element => { if(element === sell.cod_prod) {isValid = true} })
    return isValid
}

const checkCoberturaNA = (sell) => {
    let isValid = false
    const fam = ['200', '210', '245', '255', '310', '360', '437', '033']
    fam.map(element => { if(element === sell.fam_prod) {isValid = true} })
    return isValid
}

const checkCoberturaCervejas600 = (sell) => {
    let isValid = false
    const fam = ['001', '004', '081']
    fam.map(element => { if(element === sell.fam_prod) {isValid = true} })
    return isValid
}

const checkCoberturaCervejasTotal = (sell) => {
    let isValid = false
    const fam = ['001', '003', '004', '010', '025', '030', '040', '060', '061', '081']
    fam.map(element => { if(element === sell.fam_prod) {isValid = true} })
    return isValid
}

const checkSell = (sell) => {
    let isValid = false
    if(sell['Ocorr'] === '001' || sell['Ocorr'] === '002' || sell['Ocorr'] === '004') {
        if(sell['Canc'] === '000' || sell['Canc'] === '008') {
            if(sell['Cat Prod'] != '99') {
                isValid = true
            }
        }
    }

    return isValid
}

async function run() {
    try {
        //await sellModel.deleteMany({})

        fs.createReadStream('PEDIDOS.CSV')
            .pipe(csv.parse({ delimiter: '', headers: true }))
            .on('data', async (row) => {
                try {
                    if (checkSell(row)) {
                        const sell = {
                            num_ped: row['Pedido'],
                            seq_ped: row['||Item'].replace('||', ''),
                            client: row['Cliente'],
                            seller: row['Vendedor'],
                            status: row['Canc'],
                            dt_sell: toDate(row['Dt.Pedido']),
                            cat_prod: row['Cat Prod'],
                            fam_prod: row['Fam'],
                            cod_prod: row['Cod Red'],
                            sell_value: calculateSellValue(row['Vlr Total'].replace(/,/g, '.'), row['Vlr Retorno'].replace(/,/g, '.')),
                            sell_quant: calculateSellQuantity(row['Quant CX'], row['QTD/UND'], row['Quant UN'], row['Retorno CX'], row['Retorno UN']),
                            oc: row['Ocorr']
                        }
                        const query = { num_ped: sell.num_ped, seq_ped: sell.seq_ped }
                        await sellModel.findOneAndUpdate(query, sell, { upsert: true, new: true }).exec()
                    }
                } catch (error) {
                    console.log('error: ', error)
                }
            })
    } catch (error) {
        console.log('deu error: ', error)
    }
}

//run()

async function calculaFaturamento() {
    let faturamento = 0
    let volume_300_600_1lt = 0
    let volume_premium = 0
    let volume_volume_total = 0
    let volume_curinga01 = 0
    let volume_curinga02 = 0
    let clientesCobTotal = []
    let clientesCobCervejasTotal = []
    let clientesCobCervejas600 = []
    let clientesCobNA = []
    let clientesCobSchin600 = []
    let clientesCobSchinTotal = []
    let clientesCobEisenbahn600 = []
    let clientesCobEisenbahnTotal = []
    let clientesCobDevassa600 = []
    let clientesCobDevassaTotal = []

    if ("101") {
        const pedidos = await sellModel.find({ seller: "101" }).exec()
        pedidos.map(pedido => {
            faturamento += pedido.sell_value
            if(pedido.sell_value > 0) clientesCobTotal.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaCervejasTotal(pedido)) clientesCobCervejasTotal.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaCervejas600(pedido)) clientesCobCervejas600.push(pedido.client) 
            if(pedido.sell_value > 0 && checkCoberturaNA(pedido)) clientesCobNA.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaSchin600(pedido)) clientesCobSchin600.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaSchinTotal(pedido)) clientesCobSchinTotal.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaEisenbahn600(pedido)) clientesCobEisenbahn600.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaEisenbahnTotal(pedido)) clientesCobEisenbahnTotal.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaDevassa600(pedido)) clientesCobDevassa600.push(pedido.client)
            if(pedido.sell_value > 0 && checkCoberturaDevassaTotal(pedido)) clientesCobDevassaTotal.push(pedido.client)

            if(checkVolume_600_300_1lt(pedido)) volume_300_600_1lt += pedido.sell_quant
            if(checkVolume_premium(pedido)) volume_premium += pedido.sell_quant
            if(checkVolume_volume_total(pedido)) volume_volume_total += pedido.sell_quant
            if(checkKVolume_curinga01(pedido)) volume_curinga01 += pedido.sell_quant
            if(checkKVolume_curinga02(pedido)) volume_curinga02 += pedido.sell_quant
        })

        const dados = {
            faturamento:parseFloat(faturamento).toFixed(2),
            clientesCobTotal:removeDuplcates(clientesCobTotal),
            clientesCobCervejasTotal:removeDuplcates(clientesCobCervejasTotal),
            clientesCobCervejas600:removeDuplcates(clientesCobCervejas600),
            clientesCobNA: removeDuplcates(clientesCobNA),
            clientesCobSchin600: removeDuplcates(clientesCobSchin600),
            clientesCobSchinTotal: removeDuplcates(clientesCobSchinTotal),
            clientesCobEisenbahn600: removeDuplcates(clientesCobEisenbahn600),
            clientesCobEisenbahnTotal: removeDuplcates(clientesCobEisenbahnTotal),
            clientesCobDevassa600:removeDuplcates(clientesCobDevassa600),
            clientesCobDevassaTotal:removeDuplcates(clientesCobDevassaTotal)
        }
        
        console.log(clientesCobTotal)
    }
}

calculaFaturamento()
/*
app.get('/get-fund-com-by-vd/:vd', async(req, res) => {
    let faturamento = 0
    if(req.params.vd) {
        const pedidos = await sellModel.find({seller:req.params.vd}).exec()
        pedidos.map(elemento => {
            faturamento += elemento.sell_value
        })
        console.log(faturamento)
    }
})
*/
/*
app.get('/import', async (req, res) => {
    await run()
    return res.json({ message: 'importação finalizada com sucesso' })
})

app.get('/getRevenuesBySeller/:codSeller', async (req, res) => {
    const value = await sellModel.aggregate([
        {
            $match: { $or: [{ status: '0' }, { status: '8' }] }
        }
    ]).exec()
    console.log('params:', req.params)
})

async function load(seller_code) {
    try {
        const value = await sellModel.aggregate([
            {
                $match: {
                    $and: [
                        { 'status': { $in: [0, 8] } },
                        { 'oc': { $in: ['001', '002', '004'] } },
                        //{'seller':seller_code}
                    ]
                }
            }, 
            {
                $group: {
                    _id: '$seller',
                    fat: {
                        $sum: '$sell_value'
                    },
                    totalCoverage: {
                        $addToSet: '$client'
                    },
                    schinCoverage: {
                        $sum: {
                            $cond: [
                                {$eq: ['$cod_prod','902411']},
                                1,
                                0
                            ]
                        }
                    }
                    
                }
            },
            {
                $project: {
                    _id:0,
                    vendedor:"$_id",
                    valor_faturamento:"$fat",
                    cobertura_total:"$totalCoverage",
                    cobertura_schin:"$schinCoverage"
                }
            }
        ]).exec()
        console.log('params:', value)
    } catch (error) {
        console.log('error', error)
    }
}

*/

app.listen(3000, function () {
    console.log('Example app listening on port 3000!')
})